import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
METRIC_DAYS = int(os.getenv("METRIC_DAYS", 90))
ECS_METRICS = ['CPUUtilization', 'MemoryUtilization']

# --- Helper Functions ---
def format_metric_value(value, metric_name):
    if 'CPUUtilization' in metric_name or 'MemoryUtilization' in metric_name:
        return f"{value:.2f}%"
    return f"{value:.2f}"

def get_ecs_report_data(ecs_client, cloudwatch_client):
    """Fetches ECS service details and their CloudWatch metrics."""
    print("Fetching ECS service details and metrics...")
    all_services_data = []
    
    cluster_arns = ecs_client.list_clusters().get('clusterArns', [])
    for cluster_arn in cluster_arns:
        cluster_name = cluster_arn.split("/")[-1]
        
        paginator = ecs_client.get_paginator('list_services')
        for page in paginator.paginate(cluster=cluster_arn):
            service_arns = page.get('serviceArns', [])
            if not service_arns:
                continue
            
            # Describe services in batches of 10
            for i in range(0, len(service_arns), 10):
                service_batch = service_arns[i:i+10]
                services_desc = ecs_client.describe_services(cluster=cluster_arn, services=service_batch).get('services', [])
                
                for service in services_desc:
                    service_name = service['serviceName']
                    
                    # Fetch metrics for the service
                    stats = get_cloudwatch_metrics(
                        cluster_name, 
                        service_name,
                        cloudwatch_client
                    )
                    
                    all_services_data.append({
                        'Cluster': cluster_name,
                        'Service Name': service_name,
                        'ARN': service.get('serviceArn'),
                        'Status': service.get('status'),
                        'Launch Type': service.get('launchType', 'EC2'),
                        'Desired Tasks': service.get('desiredCount', 0),
                        'Running Tasks': service.get('runningCount', 0),
                        'Task Definition': service.get('taskDefinition'),
                        **stats
                    })
    
    print(f"✅ Found and processed {len(all_services_data)} ECS services.")
    return all_services_data

def get_cloudwatch_metrics(cluster_name, service_name, cloudwatch_client):
    """Fetches and calculates Min, Max, and Avg for ECS metrics over a period."""
    metrics_data = {}
    
    metric_data_queries = []
    for i, metric_name in enumerate(ECS_METRICS):
        metric_data_queries.append({
            'Id': f"m{i}",
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/ECS',
                    'MetricName': metric_name,
                    'Dimensions': [
                        {'Name': 'ClusterName', 'Value': cluster_name},
                        {'Name': 'ServiceName', 'Value': service_name}
                    ]
                },
                'Period': 300,
                'Stat': 'Average',
            },
            'ReturnData': True,
        })

    try:
        paginator = cloudwatch_client.get_paginator('get_metric_data')
        
        all_results = {metric: [] for metric in ECS_METRICS}

        for page in paginator.paginate(
            MetricDataQueries=metric_data_queries,
            StartTime=datetime.utcnow() - timedelta(days=METRIC_DAYS),
            EndTime=datetime.utcnow(),
            ScanBy='TimestampAscending'
        ):
            for result in page['MetricDataResults']:
                metric_name = ECS_METRICS[int(result['Id'][1:])]
                all_results[metric_name].extend(result['Values'])

        for metric_name, values in all_results.items():
            if values:
                metrics_data[f'{metric_name} Min'] = format_metric_value(min(values), metric_name)
                metrics_data[f'{metric_name} Max'] = format_metric_value(max(values), metric_name)
                metrics_data[f'{metric_name} Avg'] = format_metric_value(sum(values) / len(values), metric_name)
            else:
                metrics_data[f'{metric_name} Min'] = 'Nil'
                metrics_data[f'{metric_name} Max'] = 'Nil'
                metrics_data[f'{metric_name} Avg'] = 'Nil'

    except Exception as e:
        print(f"Error fetching metrics for {service_name}: {e}")
        for metric in ECS_METRICS:
            metrics_data.update({f'{metric} Min': 'Error', f'{metric} Max': 'Error', f'{metric} Avg': 'Error'})
            
    return metrics_data

# --- Main Execution ---
def main():
    try:
        session = boto3.Session(region_name=os.getenv("AWS_REGION"))
        ecs_client = session.client('ecs')
        cloudwatch_client = session.client('cloudwatch')

        print("\n--- Generating ECS Report ---")
        report_data = get_ecs_report_data(ecs_client, cloudwatch_client)
        if not report_data:
            print("No ECS services found. Exiting.")
            return

        df = pd.DataFrame(report_data)
        
        # Define and reorder columns to match the user's script
        final_columns = [
            'Cluster', 'Service Name', 'ARN', 'Status', 'Launch Type', 'Desired Tasks',
            'Running Tasks', 'Task Definition', 'CPUUtilization Min', 'CPUUtilization Max',
            'CPUUtilization Avg', 'MemoryUtilization Min', 'MemoryUtilization Max',
            'MemoryUtilization Avg'
        ]
        df = df.reindex(columns=final_columns)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"ECS_Report_{timestamp}.xlsx"

        df.to_excel(output_file, index=False)
        
        print(f"\n🚀 Report generation complete! Saved to '{output_file}'")

    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
