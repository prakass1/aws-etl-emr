######################################################################################
#File: emr_job.sh
#Description: This shell script is created to create a simple emr cluster in aws.
# In the future, I look to update the script to create teardown, and generic options.
######################################################################################



#!/bin/bash
create_emr(){
    # requires to use default roles. Advanced is to have some roles assigned for the iam to execute.
    state=true
    aws emr create-default-roles
    cluster_id=$(aws emr create-cluster --name sparkify-etl-emr --use-default-roles --release-label emr-5.35.0 --instance-count 3 --applications Name=Spark --bootstrap-actions Path="s3://user22131321-bootstrapping-emr/bootstrap.sh"  --ec2-attributes KeyName=aws-key-user,SubnetId=subnet-0d80b3d6eecf392b8 --instance-type m5.xlarge --profile udacity-de-course --query 'ClusterId' --output text)
    # --auto-terminate \
    echo " Cluster Id initiated is --- $cluster_id "
    while $state; do
        cluster_state=$(aws emr describe-cluster --cluster-id $cluster_id --profile udacity-de-course --query 'Cluster.Status.State' --output text)
        cluster_msg=$(aws emr describe-cluster --cluster-id $cluster_id --profile udacity-de-course --query 'Cluster.Status.StateChangeReason.Message' --output text)
        if [ "$cluster_state" = "RUNNING" ] || [ "$cluster_state" = "WAITING" ]; then
            state=false
            echo "Cluster is created successfully!!! and the cluster-id is $cluster_id"
        elif [ "$cluster_state" = "TERMINATED_WITH_ERRORS" ]; then
            state=false
            echo "Cluster is terminated with errors. The full error is below"
            echo $cluster_msg
        else
            echo "The current cluster bearing the id-$cluster_id has state-$cluster_state"
            echo "Sleeping for some seconds"
            sleep 30
        fi
    done
    # emr_step $cluster_id
    # Finally terminate the cluster created. Irrespective of all the errors since we do not want to encounter costs.
}

emr_step(){
    # Step to run the spark job
    local cluster_id=$1
    state=true
    step=$(aws emr add-steps --cluster-id $cluster_id --steps Type=Spark,Name="ETLSparkify",ActionOnFailure=CONTINUE,Args=[s3://emr-et-udacity13321/etl.py] --profile udacity-de-course --query 'StepIds[0]' --output text)
    echo " Step Id initiated is --- $step "
    while $state; do
        step_state=$(aws emr describe-step --cluster-id $cluster_id --step-id $step --profile udacity-de-course --query 'Step.Status.State' --output text)
        if [ "$step_state" = "COMPLETED" ]; then
            state=false
            echo "Job is done successfully!!! and the step-id is $step"
        elif [ "$cluster_state" = "FAILED" ]; then
            state=false
            echo "Job has FAILED"
        else
            echo "The current step bearing the id-$step has state-$step_state"
            echo "Sleeping for some seconds"
            sleep 30
        fi
    done
}



# Creates the emr, run a spark step and terminates.
create_emr