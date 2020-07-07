use crate::db::sql;
use crate::dict::RangeParams;

use postgres_types::ToSql;

fn query_append(target: Target, append: &str) -> String {
    let (query, append) = match target {
        Target::Dataset => (sql::LIST_DATASETS, dec_placeholders(append)),
        Target::Partition => (sql::LIST_PARTITIONS, append.into()),
    };
    format!("{} {};", query, append)
}

// Depending on the Target (Dataset or Partition) passed in to the `create` function, different
// placeholders need to be used. This is because a Partition query is always based on a `dataset_id`
// and a Dataset query has no such filter. Therefore the Partition query has an addition placeholder
// that needs to be withheld from the Dataset query, and each placeholder's position must be shifted
// down by one.
fn dec_placeholders(v: &str) -> String {
    if v.contains("$6") {
        panic!("src/db/range_query.rs: `fn dec_placeholders(v: &str) -> String {...}` must be updated to handle additional placeholders.");
    }

    v.replace("$2", "$1")
        .replace("$3", "$2")
        .replace("$4", "$3")
        .replace("$5", "$4")
}

#[test]
fn test_dec_placeholders() {
    let cases = &[
        (
            "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER LIMIT $5::INTEGER",
            "AND created_at BETWEEN $1::TIMESTAMPTZ AND $2::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $3::INTEGER LIMIT $4::INTEGER",
        ),
        (
            "AND created_at >= $2 OFFSET $3::INTEGER ORDER BY created_at ASC LIMIT $4::INTEGER",
            "AND created_at >= $1 OFFSET $2::INTEGER ORDER BY created_at ASC LIMIT $3::INTEGER",
        ),
        (
            "OFFSET $2::INTEGER ORDER BY created_at ASC LIMIT $3::INTEGER",
            "OFFSET $1::INTEGER ORDER BY created_at ASC LIMIT $2::INTEGER",
        ),
        (
            "AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC",
            "AND created_at <= $1::TIMESTAMPTZ ORDER BY created_at ASC",
        ),
    ];

    for case in cases {
        assert_eq!(String::from(case.1), dec_placeholders(case.0));
    }
}

#[test]
#[should_panic]
fn test_dec_placeholders_panic() {
    let _ = dec_placeholders("OFFSET $5::INTEGER ORDER BY created_at ASC LIMIT $6::INTEGER");
}

#[derive(Debug)]
pub enum Target {
    Dataset,
    Partition,
}

pub fn create(
    target: Target,
    params: Option<RangeParams>,
) -> (String, Vec<Box<(dyn ToSql + Sync + Send)>>) {
    if let Some(params) = params {
        match (params.start, params.end, params.count, params.offset) {
        (None, None, None, None) => (query_append(target, "ORDER BY created_at ASC"), vec![]),
        (Some(start), None, None, None) => (
            query_append(target, "AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(start)],
        ),
        (Some(start), Some(end), None, None) => (
            query_append(target, "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(start), Box::new(end)],
        ),
        (None, Some(end), None, None) => (
            query_append(target, "AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC"),
            vec![Box::new(end)],
        ),
        (None, None, Some(count), None) => {
            (query_append(target, "ORDER BY created_at ASC LIMIT $2::INTEGER"), vec![Box::new(count)])
        }
        (None, None, None, Some(offset)) => {
            (query_append(target, "ORDER BY created_at ASC OFFSET $2::INTEGER"), vec![Box::new(offset)])
        }
        (None, None, Some(count), Some(offset)) => (
            query_append(target, "ORDER BY created_at ASC OFFSET $2::INTEGER LIMIT $3::INTEGER"),
            vec![Box::new(offset), Box::new(count)],
        ),
        (Some(start), None, Some(count), Some(offset)) => (
            query_append(target, "AND created_at >= $2 OFFSET $3::INTEGER ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(start), Box::new(offset), Box::new(count)],
        ),
        (Some(start), Some(end), Some(count), Some(offset)) => (
            query_append(target, "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER LIMIT $5::INTEGER"),
            vec![
                Box::new(start),
                Box::new(end),
                Box::new(offset),
                Box::new(count),
            ],
        ),
        (None, Some(end), Some(count), Some(offset)) => (
            query_append(target, "AND created_at <= $2::TIMESTAMPTZ OFFSET $3::INTEGER ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(end), Box::new(offset), Box::new(count)],
        ),
        (Some(start), Some(end), Some(count), None) => (
            query_append(target,
                "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER",
            ),
            vec![Box::new(start), Box::new(end), Box::new(count)],
        ),
        (Some(start), Some(end), None, Some(offset)) => (
            query_append(target,
                "AND created_at BETWEEN $2::TIMESTAMPTZ AND $3::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER",
            ),
            vec![Box::new(start), Box::new(end), Box::new(offset)],
        ),
        (Some(start), None, Some(count), None) => (
            query_append(target, "AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(start), Box::new(count)],
        ),
        (Some(start), None, None, Some(offset)) => (
            query_append(target, "AND created_at >= $2::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER"),
            vec![Box::new(start), Box::new(offset)],
        ),
        (None, Some(end), Some(count), None) => (
            query_append(target, "AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC LIMIT $4::INTEGER"),
            vec![Box::new(end), Box::new(count)],
        ),
        (None, Some(end), None, Some(offset)) => (
            query_append(target, "AND created_at <= $2::TIMESTAMPTZ ORDER BY created_at ASC OFFSET $4::INTEGER"),
            vec![Box::new(end), Box::new(offset)],
        ),
       }
    } else {
        create(target, Some(Default::default()))
    }
}
