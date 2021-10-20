
import sys
import re

def regexify_name_templates(config:dict) -> dict:
    if 'name_template' in config.ecc:
        config.ecc.name_regex = config.ecc.name_template.format("(\d+)")
    elif 'queues' not in config:
        print("Need to configure either a single ecc.name_regex or define some queues")
        sys.exit(1)
    else:
        for queue in config.queues:
            config.queues[queue].name_regex = config.queues[queue].name_template.format("(\d+)")

    return config
