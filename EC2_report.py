import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
METRIC_DAYS = int(os.getenv("METRIC_DAYS", 90))
EC2_METRICS = [
    'CPUCreditUsage',
    'CPUUtilization',
    'MetadataNoToken',
    'CPUCreditBalance'
]

# --- Helper Functions ---
def get_instance_name(instance):
    for tag in instance.get('Tags', []):
        if tag['Key'] == 'Name':
            return tag['Value']
    return 'N/A'

def format_metric_value(value, metric_name):
    if 'CPUUtilization' in metric_name:
        return f"{value:.2f}%"
    elif 'Bytes' in metric_name:
        return f"{value/1e9:.2f} GB"
    return f"{value:.2f}"

# --- EC2 Functions ---
def get_ec2_instance_details(ec2_client, cloudwatch_client):
    print("Fetching EC2 instance details...")
    statuses = ec2_client.describe_instance_status(IncludeAllInstances=True).get('InstanceStatuses', [])
    status_map = {s['InstanceId']: s for s in statuses}
    addresses = ec2_client.describe_addresses().get('Addresses', [])
    ip_map = {a.get('InstanceId'): a['PublicIp'] for a in addresses if 'InstanceId' in a}
    
    instances_details = []
    paginator = ec2_client.get_paginator('describe_instances')
    for page in paginator.paginate(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}]):
        for r in page['Reservations']:
            for i in r['Instances']:
                instance_id = i.get('InstanceId')
                status = status_map.get(instance_id)
                status_check = "2/2 checks passed" if status and status.get('SystemStatus', {}).get('Status') == 'ok' and status.get('InstanceStatus', {}).get('Status') == 'ok' else "N/A"
                
                alarms = cloudwatch_client.describe_alarms_for_metric(MetricName='CPUUtilization', Namespace='AWS/EC2', Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}])
                alarm_status = "View alarms" if alarms.get('MetricAlarms') else "No alarms"

                instances_details.append({
                    'Name': get_instance_name(i),
                    'Instance ID': instance_id,
                    'Instance state': i.get('State', {}).get('Name'),
                    'Instance type': i.get('InstanceType'),
                    'Status check': status_check,
                    'Alarm status': alarm_status,
                    'Availability Zone': i.get('Placement', {}).get('AvailabilityZone'),
                    'Public IPv4 DNS': i.get('PublicDnsName', '–'),
                    'Public IPv4 address': i.get('PublicIpAddress', '–'),
                    'Elastic IP': ip_map.get(instance_id, '–'),
                    'IPv6 IPs': ', '.join(addr.get('Ipv6Address', '') for addr in i.get('Ipv6Addresses', [])),
                    'Monitoring': i.get('Monitoring', {}).get('State'),
                    'Security group name': ', '.join([sg['GroupName'] for sg in i.get('SecurityGroups', [])]),
                    'Key name': i.get('KeyName', '–'),
                    'Launch time': i.get('LaunchTime').strftime("%Y/%m/%d %H:%M GMT+5:30"),
                    'Platform details': i.get('PlatformDetails', 'Linux/UNIX'),
                    'Managed': 'FALSE',  # Placeholder
                    'Operator': '–',  # Placeholder
                    'Hostname type': f"ip-name: {i.get('PrivateDnsName', '')}",
                    'AWS Compute Optimizer finding': 'Enable Opt In'  # Placeholder
                })
    print(f"✅ Found {len(instances_details)} EC2 instances.")
    return instances_details

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
        ec2_client = session.client('ec2')
        cloudwatch_client = session.client('cloudwatch')

        print("\n--- Generating EC2 Report ---")
        details_list = get_ec2_instance_details(ec2_client, cloudwatch_client)
        if not details_list:
            print("No EC2 instances found. Exiting.")
            return

        metrics_list = [get_cloudwatch_metrics('AWS/EC2', [{'Name': 'InstanceId', 'Value': d['Instance ID']}], d['Name'], EC2_METRICS, cloudwatch_client) for d in details_list]
        
        details_df = pd.DataFrame(details_list)
        metrics_df = pd.DataFrame(metrics_list)

        print("\n--- EC2 Metrics (Min, Max, Avg) ---")
        print(metrics_df.to_string())

        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"EC2_Report_{timestamp}.xlsx"

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            details_df.to_excel(writer, sheet_name='EC2_Report', index=False)
            metrics_df.to_excel(writer, sheet_name='EC2_Report', index=False, startrow=len(details_df) + 3)
        
        print(f"\n🚀 Report generation complete! Saved to '{output_file}'")

    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
