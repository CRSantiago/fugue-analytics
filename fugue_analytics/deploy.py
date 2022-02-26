from prefect.deployments import DeploymentSpec

# note that deployment names are 
# stored and referenced as '<flow name>/<deployment name>'
DeploymentSpec(
    flow_location="/Users/kevinkho/Work/fugue-analytics/fugue_analytics/eee.py",
    name="my-first-deployment",
)