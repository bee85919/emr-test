"""
목적
아마존 EMR API와 함께 AWS SDK for Python (Boto3)을 사용하는 방법을 보여줍니다.
이것은 클러스터를 생성하고 관리하며 작업 단계를 처리합니다.
"""

import logging
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


# snippet 시작: [python.example_code.emr.RunJobFlow]
def run_job_flow(
    name,
    log_uri,
    keep_alive,
    applications,
    job_flow_role,
    service_role,
    security_groups,
    steps,
    emr_client,
):
    """
    지정된 단계로 작업 흐름을 실행합니다. 작업 흐름은 인스턴스의 클러스터를 생성하고
    클러스터에 실행할 단계를 추가합니다. 클러스터에 추가된 단계는 클러스터가 준비되면
    즉시 실행됩니다.

    이 예제는 'emr-5.30.1' 릴리스를 사용합니다. 최근 릴리스 목록은 여기에서 확인할 수 있습니다:
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html.

    :param name: 클러스터의 이름.
    :param log_uri: 로그가 저장되는 URI. 이는 Amazon S3 버킷 URL일 수 있습니다,
                    예: 's3://my-log-bucket'.
    :param keep_alive: True일 경우, 모든 단계가 실행된 후 클러스터가 대기 상태로 유지됩니다.
                       False일 경우, 단계 큐가 비어 있으면 클러스터가 자체적으로 종료됩니다.
    :param applications: 클러스터의 각 인스턴스에 설치할 응용 프로그램,
                         예: Hive 또는 Spark.
    :param job_flow_role: 클러스터에 의해 가정된 IAM 역할.
    :param service_role: 서비스에 의해 가정된 IAM 역할.
    :param security_groups: 클러스터 인스턴스에 할당할 보안 그룹.
                            Amazon EMR은 이러한 그룹에 필요한 모든 규칙을 추가하므로,
                            기본 규칙만 필요한 경우 비워 둘 수 있습니다.
    :param steps: 클러스터에 추가할 작업 흐름 단계. 이러한 단계는 클러스터가 준비되면
                  순서대로 실행됩니다.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    :return: 새로 생성된 클러스터의 ID.
    """
    try:
        response = emr_client.run_job_flow(
            Name=name,
            LogUri=log_uri,
            ReleaseLabel="emr-5.30.1",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 3,
                "KeepJobFlowAliveWhenNoSteps": keep_alive,
                "EmrManagedMasterSecurityGroup": security_groups["manager"].id,
                "EmrManagedSlaveSecurityGroup": security_groups["worker"].id,
            },
            Steps=[
                {
                    "Name": step["name"],
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            step["script_uri"],
                            *step["script_args"],
                        ],
                    },
                }
                for step in steps
            ],
            Applications=[{"Name": app} for app in applications],
            JobFlowRole=job_flow_role.name,
            ServiceRole=service_role.name,
            EbsRootVolumeSize=10,
            VisibleToAllUsers=True,
        )
        cluster_id = response["JobFlowId"]
        logger.info("Created cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't create cluster.")
        raise
    else:
        return cluster_id
# snippet 끝: [python.example_code.emr.RunJobFlow]


# snippet 시작: [python.example_code.emr.DescribeCluster]
def describe_cluster(cluster_id, emr_client):
    """
    클러스터에 대한 자세한 정보를 가져옵니다.

    :param cluster_id: 설명할 클러스터의 ID.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    :return: 검색된 클러스터 정보.
    """
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster = response["Cluster"]
        logger.info("Got data for cluster %s.", cluster["Name"])
    except ClientError:
        logger.exception("Couldn't get data for cluster %s.", cluster_id)
        raise
    else:
        return cluster
# snippet 끝: [python.example_code.emr.DescribeCluster]


# snippet 시작: [python.example_code.emr.TerminateJobFlows]
def terminate_cluster(cluster_id, emr_client):
    """
    클러스터를 종료합니다. 이 작업은 클러스터의 모든 인스턴스를 종료하며
    취소할 수 없습니다. Amazon S3 버킷과 같은 다른 곳에 저장되지 않은 모든 데이터는
    손실됩니다.

    :param cluster_id: 종료할 클러스터의 ID.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    """
    try:
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise
# snippet 끝: [python.example_code.emr.TerminateJobFlows]


# snippet 시작: [python.example_code.emr.AddJobFlowSteps]
def add_step(cluster_id, name, script_uri, script_args, emr_client):
    """
    지정된 클러스터에 작업 단계를 추가합니다. 이 예제에서는 Spark
    단계를 추가하는데, 이는 클러스터에 추가되자마자 실행됩니다.

    :param cluster_id: 클러스터의 ID.
    :param name: 단계의 이름.
    :param script_uri: Python 스크립트가 저장된 URI.
    :param script_args: Python 스크립트에 전달할 인수.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    :return: 새로 추가된 단계의 ID.
    """
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": name,
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            script_uri,
                            *script_args,
                        ],
                    },
                }
            ],
        )
        step_id = response["StepIds"][0]
        logger.info("Started step with ID %s", step_id)
    except ClientError:
        logger.exception("Couldn't start step %s with URI %s.", name, script_uri)
        raise
    else:
        return step_id
# snippet 끝: [python.example_code.emr.AddJobFlowSteps]


# snippet 시작: [python.example_code.emr.ListSteps]
def list_steps(cluster_id, emr_client):
    """
    지정된 클러스터에 대한 단계 목록을 가져옵니다. 이 예제에서는 모든 단계가
    반환되며, 완료된 단계와 실패한 단계도 포함됩니다.

    :param cluster_id: 클러스터의 ID.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    :return: 지정된 클러스터의 단계 목록.
    """
    try:
        response = emr_client.list_steps(ClusterId=cluster_id)
        steps = response["Steps"]
        logger.info("Got %s steps for cluster %s.", len(steps), cluster_id)
    except ClientError:
        logger.exception("Couldn't get steps for cluster %s.", cluster_id)
        raise
    else:
        return steps
# snippet 끝: [python.example_code.emr.ListSteps]


# snippet 시작: [python.example_code.emr.DescribeStep]
def describe_step(cluster_id, step_id, emr_client):
    """
    지정된 단계에 대한 자세한 정보를 가져옵니다. 이는 단계의 현재 상태를
    포함합니다.

    :param cluster_id: 클러스터의 ID.
    :param step_id: 단계의 ID.
    :param emr_client: Boto3 EMR 클라이언트 객체.
    :return: 지정된 단계에 대한 검색된 정보.
    """
    try:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        step = response["Step"]
        logger.info("Got data for step %s.", step_id)
    except ClientError:
        logger.exception("Couldn't get data for step %s.", step_id)
        raise
    else:
        return step
# snippet 끝: [python.example_code.emr.DescribeStep]