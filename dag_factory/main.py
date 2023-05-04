import os
import yaml
import argparse
from generate import Handler

def loadconfig(path: str, config: str) -> any:
    with open(path + '/' + config) as f:
        content = f.read()
    return yaml.safe_load(content)

def invoke(json_config: any) -> None:
    Handler(json_config).execute()

if __name__ == "__main__":
    os.environ['CONFIG_PATH'] = '/config'
    os.environ['TEMPLATE_PATH'] = '/templates'
    os.environ['DAG_RAW_TEMPLATE'] = 'template_raw.j2'
    os.environ['DAG_TRUSTED_TEMPLATE'] = 'template_trusted.j2'
    os.environ['DAG_FOLDER'] = 'dags/generated/'

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", nargs='+', help="Dag config filename(s)", required=True)
    parser.add_argument("-o", "--output", help="Output path for generated DAG files", required=True)
    args = parser.parse_args()

    path = os.path.dirname(os.path.abspath(__file__)) + os.environ['CONFIG_PATH']

    for config in args.input:
        json_config = loadconfig(path, os.path.basename(config))
        invoke(json_config)
