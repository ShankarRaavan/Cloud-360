import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
METRIC_DAYS = int(os.getenv("METRIC_DAYS", 90))
RDS_METRICS = [
    'CPUUtilization', 'FreeableMemory', 'WriteLatency', 'ReadLatency', 'Deadlocks',
    'DiskQueueDepth', 'DatabaseConnections', 'DeleteLatency', 'LoginFailures',
    'SwapUsage', 'SelectLatency', 'ReadThroughput', 'DDLLatency', 'WriteIOPS',
    'CommitLatency', 'ReadIOPS'
]

# --- Helper Functions ---
def format_metric_value(value, metric_name):
    if 'CPUUtilization' in metric_name:
        return f"{value:.2f}%"
    elif 'FreeableMemory' in metric_name:
        return f"{value/1e9:.2f} GB"
    return f"{value:.2f}"

# --- RDS Functions ---
def get_rds_instance_details(rds_client):
    print("Fetching RDS instance details...")
    db_instances = []
    paginator = rds_client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for db in page['DBInstances']:
            db_instances.append({
                'DBInstanceIdentifier': db.get('DBInstanceIdentifier'), 'DBInstanceClass': db.get('DBInstanceClass'),
                'Engine': db.get('Engine'), 'DBInstanceStatus': db.get('DBInstanceStatus'),
                'MultiAZ': db.get('MultiAZ'), 'AvailabilityZone': db.get('AvailabilityZone'),
                'StorageType': db.get('StorageType'), 'AllocatedStorage': f"{db.get('AllocatedStorage')} GB"
            })
    print(f"✅ Found {len(db_instances)} RDS instances.")
    return db_instances

def get_cloudwatch_metrics(namespace, dimensions, resource_name, metric_list, cloudwatch_client):
    print(f"Fetching CloudWatch metrics for {resource_name}...")
    metrics_data = {'Name': resource_name}
    
    metric_data_queries = []
    for i, metric_name in enumerate(metric_list):
        metric_data_queries.append({
            'Id': f"m{i}",
            'MetricStat': {
                'Metric': {
                    'Namespace': namespace,
                    'MetricName': metric_name,
                    'Dimensions': dimensions
                },
                'Period': 300,  # 5-minute granularity for better accuracy
                'Stat': 'Average',
            },
            'ReturnData': True,
        })

    try:
        paginator = cloudwatch_client.get_paginator('get_metric_data')
        
        all_results = {}
        for metric_name in metric_list:
            all_results[metric_name] = []

        for page in paginator.paginate(
            MetricDataQueries=metric_data_queries,
            StartTime=datetime.utcnow() - timedelta(days=METRIC_DAYS),
            EndTime=datetime.utcnow(),
            ScanBy='TimestampAscending'
        ):
            for result in page['MetricDataResults']:
                metric_name = metric_list[int(result['Id'][1:])]
                all_results[metric_name].extend(result['Values'])

        for metric_name, values in all_results.items():
            min_val, max_val, avg_val = ('Nil', 'Nil', 'Nil')
            if values:
                min_val = format_metric_value(min(values), metric_name)
                max_val = format_metric_value(max(values), metric_name)
                avg_val = format_metric_value(sum(values) / len(values), metric_name)
            
            metrics_data.update({
                f'{metric_name} Min': min_val,
                f'{metric_name} Max': max_val,
                f'{metric_name} Avg': avg_val
            })

    except Exception as e:
        print(f"Error fetching metrics for {resource_name}: {e}")
        for metric in metric_list:
            metrics_data.update({f'{metric} Min': 'Error', f'{metric} Max': 'Error', f'{metric} Avg': 'Error'})
            
    return metrics_data

# --- Main Execution ---
def main():
    try:
        session = boto3.Session(region_name=os.getenv("AWS_REGION"))
        rds_client = session.client('rds')
        cloudwatch_client = session.client('cloudwatch')

        print("\n--- Generating RDS Report ---")
        details_list = get_rds_instance_details(rds_client)
        if not details_list:
            print("No RDS instances found. Exiting.")
            return

        metrics_list = [get_cloudwatch_metrics('AWS/RDS', [{'Name': 'DBInstanceIdentifier', 'Value': d['DBInstanceIdentifier']}], d['DBInstanceIdentifier'], RDS_METRICS, cloudwatch_client) for d in details_list]
        
        details_df = pd.DataFrame(details_list)
        metrics_df = pd.DataFrame(metrics_list)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"RDS_Report_{timestamp}.xlsx"

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            details_df.to_excel(writer, sheet_name='RDS_Report', index=False)
            metrics_df.to_excel(writer, sheet_name='RDS_Report', index=False, startrow=len(details_df) + 3)
        
        print(f"\n🚀 Report generation complete! Saved to '{output_file}'")

    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
