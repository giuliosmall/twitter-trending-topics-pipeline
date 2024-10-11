from diagrams import Cluster, Diagram
from diagrams.aws.analytics import *
from diagrams.aws.storage import S3, S3Glacier
from diagrams.aws.management import Cloudwatch, Cloudtrail
from diagrams.aws.network import APIGateway
from diagrams.aws.security import IAM, KMS, WAF
from diagrams.aws.ml import Sagemaker
from diagrams.aws.integration import StepFunctions
from diagrams.aws.general import User
from diagrams.onprem.monitoring import Grafana

with Diagram("Twitter Trending Topics Detection System", show=False):

    # users accessing the system
    with Cluster("Clients"):
        users = [User("Business Users"), User("Data Analysts")]

    # data Ingestion Layer
    with Cluster("Ingestion Layer"):
        twitter_api = APIGateway("Twitter API")
        kinesis_stream = KinesisDataStreams("Kinesis Data Streams")

    # S3 Storage for raw and processed data
    with Cluster("Data Storage"):
        raw_data = S3("Raw Data Bucket")
        processed_data = S3("Processed Data Bucket")
        archived_data = S3Glacier("S3 Glacier Archive")

    # processing layer
    with Cluster("Data Processing"):
        with Cluster("Real-Time Processing"):
            kinesis_firehose = KinesisDataFirehose("Kinesis Firehose")
            kinesis_analytics = KinesisDataAnalytics("Kinesis Analytics")

        with Cluster("Batch Processing"):
            glue_jobs = Glue("Glue ETL Jobs")
            emr_cluster = EMR("EMR Cluster (Spark)")
        
        with Cluster("Machine Learning"):
            sagemaker = Sagemaker("SageMaker (Model Training)")
            model_registry = Sagemaker("Model Registry")

    # data transformation and quality
    with Cluster("Transformation & Enrichment"):
        glue_crawler = GlueCrawlers("Glue Crawler")
        glue_databrew = Glue("Glue DataBrew")
        redshift = Redshift("Amazon Redshift")

    # data quality and governance
    with Cluster("Data Governance"):
        lake_formation = LakeFormation("AWS Lake Formation")
        glue_catalog = GlueDataCatalog("Glue Data Catalog")
        data_quality_checks = Glue("Data Quality Checks")

    # security and compliance
    with Cluster("Security and Compliance"):
        kms = KMS("KMS Encryption")
        iam = IAM("IAM Roles and Policies")
        waf = WAF("AWS WAF")
        cloudtrail = Cloudtrail("CloudTrail")

    # orchestration
    step_functions = StepFunctions("Step Functions Orchestration")

    # monitoring and observability
    with Cluster("Monitoring and Observability"):
        cloudwatch = Cloudwatch("CloudWatch Logs/Alarms")
        grafana = Grafana("Grafana Dashboards")

    # visualization and analytics
    with Cluster("Analytics and Visualization"):
        quicksight = Quicksight("Amazon QuickSight")

    # data flow
    twitter_api >> kinesis_stream >> kinesis_firehose >> raw_data
    kinesis_stream >> kinesis_analytics >> processed_data

    raw_data >> glue_jobs >> processed_data
    processed_data >> emr_cluster >> glue_catalog

    glue_catalog >> redshift
    emr_cluster >> glue_databrew >> processed_data
    emr_cluster >> sagemaker >> model_registry

    # step Functions orchestrates batch processing
    step_functions >> glue_jobs
    step_functions >> emr_cluster

    # monitoring connections
    cloudwatch >> grafana
    emr_cluster >> cloudwatch
    kinesis_stream >> cloudtrail

    # IAM and KMS connections for data security
    iam >> glue_jobs
    kms >> raw_data
    kms >> processed_data
    waf >> twitter_api

    # user access to visualization
    users >> quicksight

    # step Functions connects to analytics
    step_functions >> quicksight