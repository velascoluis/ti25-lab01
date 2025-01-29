#!/bin/bash

# Configuration
PROJECT_ID=$1  # Project ID from first argument
REGION=$2      # Region from second argument
ZONE=$3        # Zone from third argument

if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$ZONE" ]; then
    echo "Usage: $0 <project-id> <region> <zone>"
    exit 1
fi


gcloud services enable bigqueryconnection.googleapis.com
gcloud services enable notebooks.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable documentai.googleapis.com
gcloud services enable cloudaicompanion.googleapis.com
gcloud services enable datalineage.googleapis.com
gcloud services enable datacatalog.googleapis.com


INSTANCE_NAME="gce-cdh-5-single-node"
MACHINE_TYPE="e2-standard-16"
NETWORK="vpc-main"
SUBNET="gce-snet"
CONTAINER_IMAGE="docker.io/cloudera/quickstart:latest"
GCS_CONNECTOR_URL="https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-1.7.0-hadoop2.jar"
GCS_CONNECTOR_JAR="gcs-connector-1.7.0-hadoop2.jar"

# Check for required commands
for cmd in bq jq; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd is required but not installed"
        exit 1
    fi
done

# Create scripts and config directories
if [ -d "scripts-hydrated" ]; then
    rm -rf scripts-hydrated
    echo "Removed existing scripts-hydrated directory"
fi
mkdir -p scripts-hydrated
cp *.csv scripts-hydrated/
cp *.py scripts-hydrated/
cp *.sql scripts-hydrated/

# Create core-site configuration for HDFS (without XML declaration)
cat > scripts-hydrated/core-site-add.xml << EOF
<configuration>
  <property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  </property>
  <property>
    <name>fs.gs.project.id</name>
    <value>${PROJECT_ID}</value>
  </property>
  <property>
    <name>google.cloud.auth.type</name>
    <value>SERVICE_ACCOUNT_JSON_KEYFILE</value>
  </property>
  <property>
    <name>fs.gs.auth.service.account.json.keyfile</name>
    <value>/home/cloudera/local_key.json</value>
  </property>
  <property>
    <name>fs.gs.working.dir</name>
    <value>/</value>
  </property>
  <property>
    <name>fs.gs.path.encoding</name>
    <value>uri-path</value>
  </property>
</configuration>
EOF

# Create VPC network if it doesn't exist
echo "Creating/checking VPC network..."
if ! gcloud compute networks describe $NETWORK --project=$PROJECT_ID &>/dev/null; then
    gcloud compute networks create $NETWORK \
        --project=$PROJECT_ID \
        --subnet-mode=custom
    echo "VPC network created successfully"
else
    echo "VPC network already exists"
fi

# Create subnet if it doesn't exist
echo "Creating/checking subnet..."
if ! gcloud compute networks subnets describe $SUBNET \
    --project=$PROJECT_ID \
    --region=$REGION &>/dev/null; then
    gcloud compute networks subnets create $SUBNET \
        --project=$PROJECT_ID \
        --network=$NETWORK \
        --region=$REGION \
        --range=10.0.0.0/24 \
        --enable-private-ip-google-access
    echo "Subnet created successfully"
else
    # Enable PGA on existing subnet
    gcloud compute networks subnets update $SUBNET \
        --project=$PROJECT_ID \
        --region=$REGION \
        --enable-private-ip-google-access
    echo "Subnet already exists - enabled Private Google Access"
fi

# Create firewall rules
echo "Creating/checking firewall rules..."
if ! gcloud compute firewall-rules describe gce-firewall --project=$PROJECT_ID &>/dev/null; then
    gcloud compute firewall-rules create gce-firewall \
        --project=$PROJECT_ID \
        --network=$NETWORK \
        --allow=tcp:22,tcp:7180,tcp:8888,tcp:80,tcp:50070,tcp:8088 \
        --source-ranges=0.0.0.0/0 \
        --target-tags=gce-firewall
    echo "Firewall rules created successfully"
else
    echo "Firewall rules already exist"
fi

gcloud compute firewall-rules create allow-internal-ingress \
    --project=$PROJECT_ID \
    --network=$NETWORK \
    --direction=ingress \
    --action=allow \
    --rules=tcp:0-65535,udp:0-65535 \
    --rules=all \
    --source-ranges=10.0.0.0/24 \
    --priority=0


# Create service account and assign roles
SA_NAME="cloudera-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Creating/checking service account..."
if ! gcloud iam service-accounts describe $SA_EMAIL --project=$PROJECT_ID &>/dev/null; then
    gcloud iam service-accounts create $SA_NAME \
        --display-name="Cloudera Service Account" \
        --project=$PROJECT_ID
    echo "Service account created successfully"
else
    echo "Service account already exists"
fi

# Assign necessary roles without condition
echo "Assigning IAM roles..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.admin" \
    --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataproc.worker" \
    --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.admin" \
    --condition=None

if [ ! -f scripts-hydrated/local_key.json ]; then
    echo "Creating and downloading service account key..."
    gcloud iam service-accounts keys create scripts-hydrated/local_key.json \
        --iam-account=$SA_EMAIL \
        --project=$PROJECT_ID
else
    echo "Service account key already exists"
fi

# Create a unique GCS bucket name using project ID and a timestamp
BUCKET_NAME="${PROJECT_ID}-cloudera-$(date +%s)"
echo "Creating GCS bucket: ${BUCKET_NAME}"

# Create the GCS bucket
if ! gsutil ls -b "gs://${BUCKET_NAME}" &>/dev/null; then
    gsutil mb -p ${PROJECT_ID} -l ${REGION} "gs://${BUCKET_NAME}"
    echo "GCS bucket created successfully"
    
    # Set uniform bucket-level access
    gsutil uniformbucketlevelaccess set on "gs://${BUCKET_NAME}"
    
    # Grant the service account access to the bucket
    gsutil iam ch "serviceAccount:${SA_EMAIL}:roles/storage.admin" "gs://${BUCKET_NAME}"
else
    echo "Bucket ${BUCKET_NAME} already exists"
fi

# Create custom roles for BigLake
echo "Creating custom roles for BigLake..."

# Create CustomConnectionDelegate role if it doesn't exist
CUSTOM_CONN_DELEGATE_ROLE="CustomConnectionDelegate"
echo "Creating/checking ${CUSTOM_CONN_DELEGATE_ROLE} role..."
if ! gcloud iam roles describe ${CUSTOM_CONN_DELEGATE_ROLE} --project=${PROJECT_ID} &>/dev/null; then
    gcloud iam roles create ${CUSTOM_CONN_DELEGATE_ROLE} \
        --project=${PROJECT_ID} \
        --title="Custom Connection Delegate" \
        --description="Used for BQ connections" \
        --permissions="biglake.tables.create,biglake.tables.delete,biglake.tables.get,biglake.tables.list,biglake.tables.lock,biglake.tables.update,bigquery.connections.delegate" \
        --stage=GA
    echo "Created ${CUSTOM_CONN_DELEGATE_ROLE} role"
else
    echo "Role ${CUSTOM_CONN_DELEGATE_ROLE} already exists"
fi

# Create CustomDelegate role if it doesn't exist
CUSTOM_DELEGATE_ROLE="CustomDelegate"
echo "Creating/checking ${CUSTOM_DELEGATE_ROLE} role..."
if ! gcloud iam roles describe ${CUSTOM_DELEGATE_ROLE} --project=${PROJECT_ID} &>/dev/null; then
    gcloud iam roles create ${CUSTOM_DELEGATE_ROLE} \
        --project=${PROJECT_ID} \
        --title="Custom Delegate" \
        --description="Used for BLMS connections" \
        --permissions="bigquery.connections.delegate" \
        --stage=GA
    echo "Created ${CUSTOM_DELEGATE_ROLE} role"
else
    echo "Role ${CUSTOM_DELEGATE_ROLE} already exists"
fi

# Create BigLake connection if it doesn't exist
echo "Creating/checking BigLake connection..."
BIGLAKE_CONN_NAME="biglake-connection"
BQ_LOCATION="us"  # BigQuery location

if ! bq show --connection ${PROJECT_ID}.${BQ_LOCATION}.${BIGLAKE_CONN_NAME} &>/dev/null; then
    bq mk --connection \
        --location=${BQ_LOCATION} \
        --project_id=${PROJECT_ID} \
        --connection_type=CLOUD_RESOURCE \
        ${BIGLAKE_CONN_NAME}
    echo "Created BigLake connection"
else
    echo "BigLake connection already exists"
fi

# Get the service account from the connection
BIGLAKE_SA=$(bq show --connection --format=json ${PROJECT_ID}.${BQ_LOCATION}.${BIGLAKE_CONN_NAME} | jq -r '.cloudResource.serviceAccountId')

if [ -z "$BIGLAKE_SA" ]; then
    echo "Error: Could not retrieve BigLake service account"
    exit 1
fi

echo "BigLake service account: ${BIGLAKE_SA}"

# Grant necessary permissions to the BigLake service account
echo "Granting permissions to BigLake service account..."

# Remove existing bindings first to avoid condition conflicts
echo "Removing existing IAM bindings for BigLake service account..."
gcloud projects remove-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${BIGLAKE_SA}" \
    --role="roles/storage.admin" \
    --all &>/dev/null || true

gcloud projects remove-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${BIGLAKE_SA}" \
    --role="projects/${PROJECT_ID}/roles/${CUSTOM_CONN_DELEGATE_ROLE}" \
    --all &>/dev/null || true

# Add new bindings without conditions
echo "Adding new IAM bindings..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${BIGLAKE_SA}" \
    --role="roles/storage.admin" \
    --condition=None

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${BIGLAKE_SA}" \
    --role="projects/${PROJECT_ID}/roles/${CUSTOM_CONN_DELEGATE_ROLE}" \
    --condition=None

echo "BigLake setup completed successfully"

# Create or update GCE instance
echo "Creating/checking GCE instance with Cloudera container..."
if ! gcloud compute instances describe $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE &>/dev/null; then
    echo "Creating new instance with container..."
    gcloud compute instances create-with-container $INSTANCE_NAME \
        --project=$PROJECT_ID \
        --zone=$ZONE \
        --machine-type=$MACHINE_TYPE \
        --network=$NETWORK \
        --subnet=$SUBNET \
        --boot-disk-size=200GB \
        --boot-disk-type=pd-ssd \
        --container-image=$CONTAINER_IMAGE \
        --container-privileged \
        --container-stdin \
        --container-tty \
        --container-restart-policy=always \
        --container-command="/usr/bin/docker-quickstart" \
        --tags=gce-firewall
else
    echo "Instance exists, updating container configuration..."
    gcloud compute instances stop $INSTANCE_NAME \
        --project=$PROJECT_ID \
        --zone=$ZONE \
        --quiet
    
    echo "Updating container configuration..."
    gcloud compute instances update-container $INSTANCE_NAME \
        --project=$PROJECT_ID \
        --zone=$ZONE \
        --container-image=$CONTAINER_IMAGE \
        --container-privileged \
        --container-stdin \
        --container-tty \
        --container-restart-policy=always \
        --container-command="/usr/bin/docker-quickstart"
    
    gcloud compute instances start $INSTANCE_NAME \
        --project=$PROJECT_ID \
        --zone=$ZONE \
        --quiet
fi

# Wait for instance and container to be ready
echo "Waiting for instance and container to be ready..."
for i in {1..50}; do
    echo "Checking instance status... ($i/50)"
    
    # Check if instance is running
    INSTANCE_STATUS=$(gcloud compute instances describe $INSTANCE_NAME \
        --project=$PROJECT_ID \
        --zone=$ZONE \
        --format="get(status)" 2>/dev/null)
    
    if [[ "$INSTANCE_STATUS" != "RUNNING" ]]; then
        echo "Instance is not running yet (status: $INSTANCE_STATUS)"
        sleep 10
        continue
    fi
    
    # Try to SSH and check container
    if gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
            sudo docker ps --filter 'ancestor=$CONTAINER_IMAGE' --format '{{.Status}}' | grep -q 'Up'
        " &>/dev/null; then
        echo "Container is running"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "Timeout waiting for container. Checking logs..."
        gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
            echo '=== Container Status ==='
            sudo docker ps -a
            echo '=== Container Logs ==='
            sudo docker logs \$(sudo docker ps -aq | head -1) 2>&1 || echo 'No logs available'
            echo '=== System Logs ==='
            sudo journalctl -u google-container-daemon -n 50 --no-pager
        "
        exit 1
    fi
    
    echo "Waiting for container to be ready..."
    sleep 10
done

# Restart container to fix hostname
echo "Restarting container to fix hostname..."
gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
    set -e
    CONTAINER_ID=\$(sudo docker ps -q --filter 'ancestor=$CONTAINER_IMAGE' | head -1)
    
    if [ -z \"\$CONTAINER_ID\" ]; then
        echo 'Error: Container not found'
        exit 1
    fi
    
    echo \"Stopping container \$CONTAINER_ID\"
    sudo docker stop \$CONTAINER_ID
    
    echo \"Starting container with hostname quickstart.cloudera...\"
    sudo docker run --hostname=quickstart.cloudera --privileged=true \
        -e GOOGLE_CLOUD_PROJECT=$PROJECT_ID -t -i -d $CONTAINER_IMAGE /usr/bin/docker-quickstart
    
    NEW_CONTAINER_ID=\$(sudo docker ps -q --filter 'ancestor=$CONTAINER_IMAGE' | head -1)
    echo \"Container started with new ID: \$NEW_CONTAINER_ID\"
"

# Upload files to instance
echo "Uploading files to instance..."
gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
    sudo mkdir -p /tmp/cloudera-setup
    sudo chmod 777 /tmp/cloudera-setup
    
    # Download GCS connector
    echo 'Downloading GCS connector...'
    curl -o /tmp/cloudera-setup/$GCS_CONNECTOR_JAR '$GCS_CONNECTOR_URL'
"

gcloud compute scp --project=$PROJECT_ID --zone=$ZONE \
    scripts-hydrated/* $INSTANCE_NAME:/tmp/cloudera-setup/ || {
    echo "Error uploading files to instance"
    exit 1
}

# Configure container
echo "Configuring container..."
gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
    set -e
    CONTAINER_ID=\$(sudo docker ps -q --filter 'ancestor=$CONTAINER_IMAGE' | head -1)
    
    if [ -z \"\$CONTAINER_ID\" ]; then
        echo 'Error: Container not found'
        exit 1
    fi
    
    # Create directories in container
    echo 'Creating directories in container...'
    sudo docker exec \$CONTAINER_ID mkdir -p /home/cloudera /etc/hadoop/conf
    
    # Copy files to container
    echo 'Copying files to container...'
    for file in /tmp/cloudera-setup/*; do
        echo \"Copying \$file...\"
        sudo docker cp \$file \$CONTAINER_ID:/home/cloudera/
    done
    
    # Set correct permissions for local_key.json
    sudo docker exec \$CONTAINER_ID chmod 644 /home/cloudera/local_key.json
    
    # Copy GCS connector to Hadoop lib
    echo 'Copying GCS connector to Hadoop lib...'
    sudo docker cp /tmp/cloudera-setup/$GCS_CONNECTOR_JAR \$CONTAINER_ID:/usr/lib/hadoop/lib/
    
    # Configure Hadoop
    echo 'Configuring Hadoop...'
    sudo docker exec \$CONTAINER_ID bash -c '
        # Remove spaces and remove last line and append GCS connector config
        sed -i \"/^$/d\" /etc/hadoop/conf/core-site.xml
        sed -i \"$ d\" /etc/hadoop/conf/core-site.xml
        # Add connector config
        cat /home/cloudera/core-site-add.xml >> /etc/hadoop/conf/core-site.xml
        echo \"</configuration>\" >> /etc/hadoop/conf/core-site.xml
        # Restart services
        /usr/bin/docker-quickstart
    '
"

# Verify deployment
echo "Verifying deployment..."
gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
    set -e
    if ! sudo docker ps --filter 'ancestor=$CONTAINER_IMAGE' | grep -q 'Up'; then
        echo 'Error: Container is not running'
        exit 1
    fi
    
    echo 'Checking required files...'
    CONTAINER_ID=\$(sudo docker ps -q --filter 'ancestor=$CONTAINER_IMAGE' | head -1)
    
    # Check if local_key.json exists
    if ! sudo docker exec \$CONTAINER_ID test -f /home/cloudera/local_key.json; then
        echo 'Error: /home/cloudera/local_key.json not found in container'
        exit 1
    fi
    
    sudo docker exec \$CONTAINER_ID ls -l /home/cloudera/local_key.json /usr/lib/hadoop/lib/$GCS_CONNECTOR_JAR
    
    # Verify Hadoop configuration
    echo 'Verifying Hadoop configuration...'
    sudo docker exec \$CONTAINER_ID bash -c \"
        if ! grep -q 'fs.gs.impl' /etc/hadoop/conf/core-site.xml; then
            echo 'Error: Hadoop configuration not updated properly'
            exit 1
        fi
    \"
    
    # Verify data files
    echo 'Verifying data files...'
    sudo docker exec \$CONTAINER_ID ls -l /home/cloudera/customers.csv /home/cloudera/loan_applications.csv /home/cloudera/loan_repayments.csv
    
    # Start Hive Metastore service
    echo 'Starting Hive Metastore service...'
    sudo docker exec \$CONTAINER_ID bash -c \"
        service hive-metastore start
        sleep 10  # Give metastore time to start
        
        # Verify metastore is running
        if ! service hive-metastore status; then
            echo 'Error: Hive metastore failed to start'
            exit 1
        fi
    \"
    
    # Verify Hive setup
    echo 'Verifying Hive setup...'
    sudo docker exec \$CONTAINER_ID bash -c \"
        # Set JAVA_HOME
        export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
        export PATH=\$JAVA_HOME/bin:\$PATH
        
        # Start HDFS if not running
        service hadoop-hdfs-namenode start
        service hadoop-hdfs-datanode start
        
        # Wait for HDFS to be available
        until hdfs dfs -ls / &>/dev/null; do
            echo 'Waiting for HDFS to be available...'
            sleep 5
        done
        
        # Create and set permissions for Hive warehouse directory
        hdfs dfs -mkdir -p /user/hive/warehouse
        hdfs dfs -chmod g+w /user/hive/warehouse
        hdfs dfs -chown -R hive:hive /user/hive
        
        # Start the Hive metastore
        service hive-metastore restart
        sleep 5
        
        # Test Hive
        hive -e 'SHOW DATABASES;'
    \"
"

# Run Hive population script
echo 'Populating Hive tables...'
gcloud compute ssh $INSTANCE_NAME --project=$PROJECT_ID --zone=$ZONE --command="
    set -e
    CONTAINER_ID=\$(sudo docker ps -q --filter 'ancestor=$CONTAINER_IMAGE' | head -1)
    
    sudo docker exec \$CONTAINER_ID bash -c '
        # Set JAVA_HOME
        export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
        export PATH=\$JAVA_HOME/bin:\$PATH

        # Ensure HDFS is running
        service hadoop-hdfs-namenode start
        service hadoop-hdfs-datanode start
        
        # Wait for HDFS
        until hdfs dfs -ls / &>/dev/null; do
            echo \"Waiting for HDFS...\"
            sleep 5
        done

        # Set up proper ownership and permissions
        chown -R cloudera:cloudera /home/cloudera
        chmod 644 /home/cloudera/*.csv
        
        # Create and set up Hive directories
        hdfs dfs -mkdir -p /user/hive/warehouse
        hdfs dfs -chmod -R 777 /user/hive/warehouse
        hdfs dfs -chown -R hive:hive /user/hive
        
        # Restart Hive metastore with proper permissions
        service hive-metastore restart
        sleep 5

        # Run the populate_hive.sql script as cloudera user
        su - cloudera -c \"
            export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
            export PATH=\\\$JAVA_HOME/bin:\\\$PATH
            hive -f /home/cloudera/populate_hive.sql
        \"

        # Verify tables were created
        echo \"Verifying table creation...\"
        su - cloudera -c \"
            export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
            export PATH=\\\$JAVA_HOME/bin:\\\$PATH
            hive -e \\\"USE ccf_db; SHOW TABLES;\\\"
        \"
    '
"

echo "BUCKET_NAME to copy data to is ${BUCKET_NAME}"
echo "Deployment complete! The instance may take a few minutes to fully start up."
echo "You can connect to the cloudera instance using:"
CDH_CONTAINER_ID=`gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE} --command 'sudo docker ps -q | head -1'`
echo "gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE} --container ${CDH_CONTAINER_ID}" 
