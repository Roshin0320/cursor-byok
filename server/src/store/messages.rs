//! Persists canonical Messages and enforces event idempotency.
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    model::{CanonicalMessage, ConversationId},
    Error, Result,
};

use super::{now_ms, Store};

// Keep the small lineage outermost; SQLite may otherwise scan every checkpoint's messages.
const CHECKPOINT_MESSAGES_SQL: &str = "
    WITH RECURSIVE lineage(checkpoint_id, parent_checkpoint_id, depth) AS (
        SELECT checkpoint_id, parent_checkpoint_id, 0
        FROM conversation_checkpoints WHERE checkpoint_id = ?
        UNION ALL
        SELECT r.checkpoint_id, r.parent_checkpoint_id, lineage.depth + 1
        FROM conversation_checkpoints r
        JOIN lineage ON r.checkpoint_id = lineage.parent_checkpoint_id
    )
    SELECT m.payload_json
    FROM lineage
    CROSS JOIN checkpoint_messages rm ON rm.checkpoint_id = lineage.checkpoint_id
    CROSS JOIN messages m
      ON m.conversation_id = rm.conversation_id AND m.message_id = rm.message_id
    ORDER BY lineage.depth DESC, rm.ordinal ASC";

impl Store {
    pub async fn message(
        &self,
        conversation_id: &ConversationId,
        message_id: &str,
    ) -> Result<Option<CanonicalMessage>> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload_json FROM messages WHERE conversation_id = ? AND message_id = ?",
        )
        .bind(conversation_id.as_str())
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
            .transpose()
    }

    pub(crate) async fn put_message_tx(
        tx: &mut Transaction<'_, Sqlite>,
        conversation_id: &ConversationId,
        message: &CanonicalMessage,
    ) -> Result<()> {
        let payload = serde_json::to_string(message)?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO messages
             (conversation_id, message_id, role, origin, payload_json, runtime_event_id, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(conversation_id.as_str())
        .bind(&message.message_id)
        .bind(role_name(&message.role))
        .bind(origin_name(&message.origin))
        .bind(&payload)
        .bind(&message.runtime_event_id)
        .bind(now_ms())
        .execute(&mut **tx)
        .await?
        .rows_affected()
            == 1;
        if inserted {
            return Ok(());
        }

        let existing: Option<String> = sqlx::query_scalar(
            "SELECT payload_json FROM messages
             WHERE conversation_id = ? AND (message_id = ? OR runtime_event_id = ?)",
        )
        .bind(conversation_id.as_str())
        .bind(&message.message_id)
        .bind(&message.runtime_event_id)
        .fetch_optional(&mut **tx)
        .await?;
        match existing {
            Some(existing) if existing == payload => Ok(()),
            Some(_) => Err(Error::Store(format!(
                "message id or runtime event reused with different content: {}",
                message.message_id
            ))),
            None => Err(Error::Store(format!(
                "message insert was ignored without an existing object: {}",
                message.message_id
            ))),
        }
    }

    pub(crate) async fn load_checkpoint_messages_tx(
        tx: &mut Transaction<'_, Sqlite>,
        checkpoint_id: i64,
    ) -> Result<Vec<CanonicalMessage>> {
        let rows = sqlx::query(CHECKPOINT_MESSAGES_SQL)
            .bind(checkpoint_id)
            .fetch_all(&mut **tx)
            .await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(row.get::<&str, _>(0)).map_err(Into::into))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Origin, Role};

    #[tokio::test]
    async fn checkpoint_history_uses_indexed_lookups_and_preserves_lineage_order() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", directory.path().join("test.db").display());
        let store = Store::connect(&url).await.unwrap();
        let conversation = ConversationId::new("history");
        let first = CanonicalMessage::text("first", Role::User, Origin::User, "first");
        let second = CanonicalMessage::text("second", Role::Assistant, Origin::Assistant, "second");
        let base = store
            .import_checkpoint(&conversation, std::slice::from_ref(&first))
            .await
            .unwrap();
        sqlx::query("UPDATE conversations SET current_checkpoint_id = ? WHERE conversation_id = ?")
            .bind(base.0)
            .bind(conversation.as_str())
            .execute(store.pool())
            .await
            .unwrap();
        let expected = vec![first.clone(), second];
        let head = store
            .import_checkpoint(&conversation, &expected)
            .await
            .unwrap();
        let unrelated = ConversationId::new("unrelated");
        store
            .import_checkpoint(&unrelated, std::slice::from_ref(&first))
            .await
            .unwrap();

        assert_eq!(
            store.load_checkpoint_messages(head).await.unwrap(),
            expected
        );
        assert_eq!(
            store.load_checkpoint_messages(base).await.unwrap(),
            vec![first]
        );
        let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {CHECKPOINT_MESSAGES_SQL}"))
            .bind(head.0)
            .fetch_all(store.pool())
            .await
            .unwrap();
        let details: Vec<String> = plan.iter().map(|row| row.get("detail")).collect();
        assert!(
            details.iter().any(
                |detail| detail.starts_with("SEARCH rm ") && detail.contains("checkpoint_id=?")
            ),
            "checkpoint history must use indexed lookups: {details:?}"
        );
        assert!(!details.iter().any(|detail| detail.starts_with("SCAN rm")));
    }
}

fn role_name(role: &crate::model::Role) -> &'static str {
    match role {
        crate::model::Role::System => "system",
        crate::model::Role::User => "user",
        crate::model::Role::Assistant => "assistant",
        crate::model::Role::Tool => "tool",
    }
}

fn origin_name(origin: &crate::model::Origin) -> &'static str {
    match origin {
        crate::model::Origin::Prompt => "prompt",
        crate::model::Origin::User => "user",
        crate::model::Origin::Runtime => "runtime",
        crate::model::Origin::Assistant => "assistant",
        crate::model::Origin::Tool => "tool",
    }
}
