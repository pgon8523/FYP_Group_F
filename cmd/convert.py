# This script is to convert a image file to base64 encoding.

import base64
import sys

with open(sys.argv[1], "rb") as img_file:
    data = base64.b64encode(img_file.read()).decode("UTF-8")
    f = open(sys.argv[2], "w")
    f.write(data)
