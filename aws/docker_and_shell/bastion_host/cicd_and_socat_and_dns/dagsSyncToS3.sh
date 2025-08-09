for ip in $(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=de6-team6-airflow-*" \
  --query "Reservations[*].Instances[*].PrivateIpAddress" \
  --output text); do
  echo "🔁 Syncing DAG on $ip ..."
  ssh -i ~/.ssh/privateAccessKey.pem ec2-user@$ip \
    "aws s3 sync s3://de6-team6-bucket/dags/ /home/ec2-user/airflow/dags/"
done
