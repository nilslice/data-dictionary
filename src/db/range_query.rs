use crate::dict::{Dataset, Partition, RangeParams};
use crate::error::Error;

use postgres::{self, row::Row};

fn partitions_from_rows(rows: Vec<Row>) -> Vec<Partition> {
    rows.iter().map(|r| Partition::from(r)).collect()
}

pub fn partitions(
    mut client: postgres::Client,
    params: &RangeParams,
    dataset: &Dataset,
) -> Result<Vec<Partition>, Error> {
    match (params.start, params.end, params.count, params.offset) {
        (None, None, None, None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1
            "#,
            )?;

            Ok(partitions_from_rows(client.query(&stmt, &[&dataset.id])?))
        }
        (Some(start), None, None, None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at >= $2
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start])?,
            ))
        }
        (Some(start), Some(end), None, None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at BETWEEN $2 AND $3
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &end])?,
            ))
        }
        (None, Some(end), None, None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at <= $2
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &end])?,
            ))
        }
        (None, None, Some(count), None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                LIMIT $2
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &count])?,
            ))
        }
        (None, None, None, Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                OFFSET $2
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &offset])?,
            ))
        }
        (None, None, Some(count), Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                OFFSET $2
                LIMIT $3
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &offset, &count])?,
            ))
        }
        (Some(start), None, Some(count), Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at >= $2
                OFFSET $3
                LIMIT $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &offset, &count])?,
            ))
        }
        (Some(start), Some(end), Some(count), Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at BETWEEN $2 AND $3
                OFFSET $4
                LIMIT $5
        "#,
            )?;

            Ok(partitions_from_rows(client.query(
                &stmt,
                &[&dataset.id, &start, &end, &offset, &count],
            )?))
        }
        (None, Some(end), Some(count), Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at <= $2    
                OFFSET $3
                LIMIT $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &end, &offset, &count])?,
            ))
        }
        (Some(start), Some(end), Some(count), None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at BETWEEN $2 AND $3
                LIMIT $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &end, &count])?,
            ))
        }
        (Some(start), Some(end), None, Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at BETWEEN $2 AND $3
                OFFSET $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &end, &offset])?,
            ))
        }
        (Some(start), None, Some(count), None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at >= $2
                LIMIT $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &count])?,
            ))
        }
        (Some(start), None, None, Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at >= $2
                OFFSET $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &start, &offset])?,
            ))
        }
        (None, Some(end), Some(count), None) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at <= $2
                LIMIT $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &end, &count])?,
            ))
        }
        (None, Some(end), None, Some(offset)) => {
            let stmt = client.prepare(
                r#"
                SELECT partition_id, partition_name, dataset_id, created_at, updated_at
                FROM partitions
                WHERE dataset_id = $1 
                AND created_at <= $2
                OFFSET $4
            "#,
            )?;

            Ok(partitions_from_rows(
                client.query(&stmt, &[&dataset.id, &end, &offset])?,
            ))
        }
    }
}
