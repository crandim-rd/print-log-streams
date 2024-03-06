// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::result_large_err)]

use aws_config::Region;
use aws_sdk_cloudwatchlogs::{meta::PKG_VERSION, types::LogStream, Client};
use clap::Parser;

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
}

async fn get_streams(
    client: &aws_sdk_cloudwatchlogs::Client,
    log_group_name: &str,
) -> Result<Vec<LogStream>, aws_sdk_cloudwatchlogs::Error> {
    let streams_result = client
        .describe_log_streams()
        .log_group_name(log_group_name)
        .into_paginator()
        .items()
        .send()
        .collect::<Vec<_>>()
        .await;

    let mut streams: Vec<LogStream> = Vec::new();

    for result in streams_result {
        match result {
            Ok(log_stream) => streams.push(log_stream), // 成功した場合はVecに追加
            Err(e) => println!("Warning: Failed to retrieve a log stream: {:?}", e), // エラーの場合は警告を表示
        }
    }
    println!("Found {} streams:", streams.len());
    Ok(streams)
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
/// # Returns
///
#[tokio::main]
async fn main() -> Result<(), aws_sdk_cloudwatchlogs::Error> {
    let Opt {
        region,
        group,
        verbose,
        profile_name,
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
    let streams = get_streams(&client, &group).await?;
    println!("Found {} streams:", streams.len());

    for stream in streams.into_iter() {
        if let Some(stream_name) = stream.log_stream_name() {
            println!(
                "{}",
                stream_name
            );
        } else {
            println!("No stream name found");
        }
    }
    Ok(())
}
