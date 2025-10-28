import EC2_report
import RDS_report
import ECS_report

def main():
    """
    Main function to generate all AWS reports.
    """
    print("--- Starting AWS Report Generation ---")
    
    try:
        print("\n--- Generating EC2 Report ---")
        EC2_report.main()
    except Exception as e:
        print(f"❌ Error generating EC2 report: {e}")
        
    try:
        print("\n--- Generating RDS Report ---")
        RDS_report.main()
    except Exception as e:
        print(f"❌ Error generating RDS report: {e}")
        
    try:
        print("\n--- Generating ECS Report ---")
        ECS_report.main()
    except Exception as e:
        print(f"❌ Error generating ECS report: {e}")
        
    print("\n--- All Reports Generated Successfully ---")

if __name__ == "__main__":
    main()
