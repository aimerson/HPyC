# HPyC
Python package for submitting/managing HPC jobs. Currently works to manage jobs with PBS and SLURM schedulers. (SLURM module still in beta.) 

# Download and installation
Steps to download and install the package:
```
git clone https://github.com/aimerson/HPyC.git
cd HPyC
python setup.py build
python setup.py install
```
If you do not have root access you might need to modify the last step as follows:
```
python setup.py install --user
```

# Example usage
Examples of how to use the various classes can be found in the *examples* subdirectory.
