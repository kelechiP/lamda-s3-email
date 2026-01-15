def load_json_from_s3_env(env_var_name: str, default=None):
    uri = os.getenv(env_var_name)
    if not uri:
        print(f"[WARN] {env_var_name} not set; using default")
        return default if default is not None else {}

    if not uri.startswith("s3://"):
        raise ValueError(f"{env_var_name} must be an s3:// URI, got: {uri}")

    _, _, rest = uri.partition("s3://")
    bucket, _, key = rest.partition("/")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


AGENCY_EMAIL_MAP_S3 = s3://<bucket>/<path>/agency_email_map.json


AGENCY_EMAIL_MAP = load_json_from_s3_env("AGENCY_EMAIL_MAP_S3", default={})
