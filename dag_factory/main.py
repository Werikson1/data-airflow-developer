import os
import yaml
import argparse
from generate import Handler

def loadconfig(path: str, config: str) -> any:
    with open(path + '/' + config) as f:
        content = f.read()
    return yaml.safe_load(content)

def invoke(json_config: any, output_path: str) -> None:
    Handler(json_config, output_path).execute()

if __name__ == "__main__":
    os.environ['CONFIG_PATH'] = '/config'
    os.environ['TEMPLATE_PATH'] = '/templates'
    os.environ['DAG_RAW_TEMPLATE'] = 'template_raw.j2'
    os.environ['DAG_TRUSTED_TEMPLATE'] = 'template_trusted.j2'
    os.environ['DAG_FOLDER'] = 'dags/generated/'

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True, help="Input YAML file path")
    parser.add_argument("--output", type=str, required=True, help="Output DAG file path")
    args = parser.parse_args()

    path = os.path.dirname(os.path.abspath(__file__)) + os.environ['CONFIG_PATH']

    json_config = loadconfig(path, args.input)
    invoke(json_config, args.output)
