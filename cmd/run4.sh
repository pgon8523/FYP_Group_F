# This generate a dummy img file under $imgpath
# Replace it with the actual scripts in your project. 

path=$1
imgpath=$2
obj=${imgpath}/$3

# Generate a dummy img.
echo "Generating img..."
python3 ${path}/genimg.py ${obj}
echo "Done."
