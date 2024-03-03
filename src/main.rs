// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::result_large_err)]

use std::time::SystemTime;
use std::vec;

use aws_config::Region;
use aws_sdk_cloudwatch::primitives::DateTime;
use aws_sdk_cloudwatch::types::{MetricDatum, StandardUnit};
use aws_sdk_cloudwatchlogs::{meta::PKG_VERSION, types::LogStream, Client};
use chrono::{TimeZone, Utc};
use clap::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AWSError {
    #[error("CloudWatch Logs error: {0}")]
    CloudWatchLogsError(#[from] aws_sdk_cloudwatchlogs::Error),
    #[error("CloudWatch error: {0}")]
    CloudWatchError(#[from] aws_sdk_cloudwatch::Error),
}

#[derive(Debug, Parser)]
struct Opt {
    /// The AWS Region. Overrides environment variable `AWS_REGION` and profile's default region.
    #[structopt(short, long)]
    region: Option<String>,

    /// The log group name.
    #[structopt(short, long)]
    group: String,

    /// Whether to display additional information.
    #[structopt(short, long)]
    verbose: bool,

    /// The name of the AWS profile. if not supplied, uses default.
    #[structopt(short, long)]
    profile_name: Option<String>,

    /// The namespace for the metric. if not supplied, does not send metrics to CloudWatch.
    #[structopt(short, long)]
    namespace: Option<String>,
}

async fn get_streams(
    client: &aws_sdk_cloudwatchlogs::Client,
    log_group_name: &str,
) -> Result<Vec<LogStream>, aws_sdk_cloudwatchlogs::Error> {
    let resp = client
        .describe_log_streams()
        .log_group_name(log_group_name)
        .send()
        .await?;
    let streams = resp.log_streams().to_owned();
    println!("Found {} streams:", streams.len());
    Ok(streams)
}

// Lists the streams for a log group.
// snippet-start:[cloudwatchlogs.rust.list-log-streams]
fn count_active_streams(
    streams: &[LogStream],
    current_datetime: &chrono::DateTime<Utc>,
    duration: i64,
) -> u64 {
    let mut count: u64 = 0;

    for stream in streams {
        if let Some(last_event_timestamp) = stream.last_event_timestamp() {
            // last_event_timestamp is expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC
            let last_event_datetime = Utc.timestamp_opt(last_event_timestamp / 1000, 0).unwrap();
            if *current_datetime - chrono::Duration::days(duration) < last_event_datetime {
                count += 1;
            }
        }
    }
    println!("  {} streams updated in the last {} days", count, duration);
    count
}

// snippet-end:[cloudwatchlogs.rust.list-log-streams]

/// Lists the log streams for a log group in the Region.
/// # Arguments
///
/// * `-g LOG-GROUP` - The name of the log group.
/// * `[-r REGION]` - The Region in which the client is created.
///   If not supplied, uses the value of the **AWS_REGION** environment variable.
/// * `[-v]` - Whether to display additional information.
/// * `[-p PROFILE]` - The name of the AWS profile.
///   If not supplied, uses the default profile.
/// * `[-n NAMESPACE]` - The namespace for the metric.
///   If not supplied, does not send metrics to CloudWatch.
/// # Returns
///
#[tokio::main]
async fn main() -> Result<(), AWSError> {
    let Opt {
        region,
        group,
        verbose,
        profile_name,
        namespace,
    } = Opt::parse();

    if verbose {
        tracing_subscriber::fmt::init();
    }

    let mut config_loader = aws_config::from_env();
    if let Some(profile_name) = profile_name {
        config_loader = config_loader.profile_name(profile_name);
    }
    if let Some(region) = region {
        config_loader = config_loader.region(Region::new(region));
    }

    let shared_config = config_loader.load().await;

    if verbose {
        println!();
        println!("CloudWatchLogs client version: {}", PKG_VERSION);
        println!(
            "Region:                        {}",
            shared_config.region().unwrap().as_ref()
        );
        println!("Log group name:                {}", &group);
        println!();
    }

    let client = Client::new(&shared_config);
    let current_datetime = Utc::now();
    let streams = get_streams(&client, &group).await?;

    let count_1month = count_active_streams(&streams, &current_datetime, 30);
    let count_1week = count_active_streams(&streams, &current_datetime, 7);
    let count_1day = count_active_streams(&streams, &current_datetime, 1);

    if let Some(namespace) = namespace {
        let client = aws_sdk_cloudwatch::Client::new(&shared_config);
        let timestamp = DateTime::from(SystemTime::from(current_datetime));
        let dimention = aws_sdk_cloudwatch::types::Dimension::builder()
            .set_name(Some("LogGroupName".to_string()))
            .set_value(Some(group))
            .build();
        let metric_data = vec![
            MetricDatum::builder()
                .set_metric_name(Some("MonthlyActiveStreams".to_string()))
                .set_timestamp(Some(timestamp))
                .set_unit(Some(StandardUnit::Count))
                .set_value(Some(count_1month as f64))
                .set_dimensions(Some(vec![dimention.clone()]))
                .build(),
            MetricDatum::builder()
                .set_metric_name(Some("WeeklyActiveStreams".to_string()))
                .set_timestamp(Some(timestamp))
                .set_unit(Some(StandardUnit::Count))
                .set_value(Some(count_1week as f64))
                .set_dimensions(Some(vec![dimention.clone()]))
                .build(),
            MetricDatum::builder()
                .set_metric_name(Some("DailyActiveStreams".to_string()))
                .set_timestamp(Some(timestamp))
                .set_unit(Some(StandardUnit::Count))
                .set_value(Some(count_1day as f64))
                .set_dimensions(Some(vec![dimention.clone()]))
                .build(),
        ];
        client
            .put_metric_data()
            .set_metric_data(Some(metric_data))
            .set_namespace(Some(namespace.to_string()))
            .send()
            .await
            .map_err(aws_sdk_cloudwatch::Error::from)?;
        println!("Sent metrics to CloudWatch")
    }

    // count_active_streams(&client, &group, &current_datetime).await;
    Ok(())
}
