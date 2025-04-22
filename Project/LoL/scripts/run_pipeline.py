import argparse
import json 
import yaml
import os 
import subprocess
import sys
from importlib.util import spec_from_file_location, module_from_spec
from dotenv import load_dotenv

load_dotenv()   # load .env file in the current environment


def init():
    parser = argparse.ArgumentParser(description='Run a pipeline')
    parser.add_argument('--pipeline_path', type=str, metavar="PATH", required=True)
    parser.add_argument('--config_path', type=str, metavar="PATH", required=True)
    args = parser.parse_args()
    
    return args


def main():
    args = init()
    with open(args.config_path, 'r') as fp:
        config = yaml.safe_load(fp)
        config = config if config is not None else {}
    
    pipeline_path = os.path.dirname(os.path.abspath(args.pipeline_path))        # full_path/to/pipelines/default
    pipeline_name = os.path.splitext(os.path.basename(args.pipeline_path))[0]   # pipeline

    # 구 버전 동적 로딩 방식
    sys.path.append(pipeline_path)
    pipeline = getattr(__import__(pipeline_name), 'Pipeline')(**config)
    sys.path.pop()
    del sys.modules[pipeline_name]

    # 새로운 방식
    # spec = spec_from_file_location(pipeline_name, pipeline_path)
    # module = module_from_spec(spec)
    # spec.loader.exec_module(module)
    # pipeline = module.Pipeline(**config)    
    
    pipeline()
    
    
if __name__ == '__main__':
    main()