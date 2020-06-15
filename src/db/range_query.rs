use crate::dict::RangeParams;
use postgres::types::ToSql;

const SQL_ALL_PARTITIONS: &str = r#"
    SELECT partition_id, partition_name, partition_url, dataset_id, created_at, updated_at
    FROM partitions
    WHERE dataset_id = $1
"#;

fn query_append(append: &str) -> String {
    format!("{} {};", SQL_ALL_PARTITIONS, append)
}

pub fn partitions(params: &RangeParams) -> (String, Vec<Box<(dyn ToSql + Sync)>>) {
    match (params.start, params.end, params.count, params.offset) {
        (None, None, None, None) => (query_append("ORDER BY created_at ASC"), vec![]),
        (Some(start), None, None, None) => (
            query_append("AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(start)],
        ),
        (Some(start), Some(end), None, None) => (
            query_append("AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(start), Box::new(end)],
        ),
        (None, Some(end), None, None) => (
            query_append("AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(end)],
        ),
        (None, None, Some(count), None) => {
            (query_append(" ORDER BY created_at ASC LIMIT $2::INTEGER"), vec![Box::new(count)])
        }
        (None, None, None, Some(offset)) => {
            (query_append(" ORDER BY created_at ASC OFFSET $2::INTEGER"), vec![Box::new(offset)])
        }
        (None, None, Some(count), Some(offset)) => (
            query_append("OFFSET $2::INTEGER ORDER BY created_at ASC LIMIT $3::INTEGER"),
            vec![Box::new(offset), Box::new(count)],
        ),
        (Some(start), None, Some(count), Some(offset)) => (
            query_append("AND created_at >= $2 OFFSET $3::INTEGER ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(start), Box::new(offset), Box::new(count)],
        ),
        (Some(start), Some(end), Some(count), Some(offset)) => (
            query_append("AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER LIMIT $5::INTEGER"),
            vec![
                Box::new(start),
                Box::new(end),
                Box::new(offset),
                Box::new(count),
            ],
        ),
        (None, Some(end), Some(count), Some(offset)) => (
            query_append("AND created_at <= $2::TIMESTAMPTZ OFFSET $3::INTEGER ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(end), Box::new(offset), Box::new(count)],
        ),
        (Some(start), Some(end), Some(count), None) => (
            query_append(
                "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER",
            ),
            vec![Box::new(start), Box::new(end), Box::new(count)],
        ),
        (Some(start), Some(end), None, Some(offset)) => (
            query_append(
                "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER",
            ),
            vec![Box::new(start), Box::new(end), Box::new(offset)],
        ),
        (Some(start), None, Some(count), None) => (
            query_append("AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(start), Box::new(count)],
        ),
        (Some(start), None, None, Some(offset)) => (
            query_append("AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER"),
            vec![Box::new(start), Box::new(offset)],
        ),
        (None, Some(end), Some(count), None) => (
            query_append("AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(end), Box::new(count)],
        ),
        (None, Some(end), None, Some(offset)) => (
            query_append("AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER"),
            vec![Box::new(end), Box::new(offset)],
        ),
    }
}
