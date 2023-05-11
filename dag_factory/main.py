import os
import yaml
import argparse
from generate import Handler

def loadconfig(path: str, config: str) -> any:
    with open(path + '/' + config) as f:
        content = f.read()
    return yaml.safe_load(content)

def invoke(json_config: any, output: str) -> None:
    Handler(json_config, output).execute()

if __name__ == "__main__":
    os.environ['CONFIG_PATH'] = '/config'
    os.environ['TEMPLATE_PATH'] = '/templates'
    os.environ['DAG_RAW_TEMPLATE'] = 'template_raw.j2'
    os.environ['DAG_TRUSTED_TEMPLATE'] = 'template_trusted.j2'
    # os.environ['DAG_TEMPLATE'] = 'template.j2'
    os.environ['DAG_FOLDER'] = 'dags/generated/'

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Dag config filename")
    parser.add_argument("-o", "--output", help="Output folder for generated DAG files", default='dags/generated/')
    args = parser.parse_args()

    output = args.output

    path = os.path.dirname(os.path.abspath(__file__)) + os.environ['CONFIG_PATH']

    if args.config is not None:
        json_config = loadconfig(path, args.config)
        invoke(json_config,output)

    for config in os.listdir(path):
        json_config = loadconfig(path, config)
        invoke(json_config, output)
