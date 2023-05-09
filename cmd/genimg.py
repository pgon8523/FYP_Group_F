# Suppose you generate the image using this script.

import numpy as np
import matplotlib.pyplot as plt
import sys

plt.figure(1, dpi=50)
x = np.linspace(-np.pi, np.pi, 100)
plt.plot(x,np.sin(x))
plt.savefig(sys.argv[1])
