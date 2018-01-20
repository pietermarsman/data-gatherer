#!/usr/bin/env python
import os

import yaml

config = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(config, "r") as f:
    settings = yaml.load(f)
