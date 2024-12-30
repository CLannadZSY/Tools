import setuptools

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name="Py-library",
    version="1.0.7",
    author="CLannadZSY",
    author_email="zsymidi@gmail.com",
    description="py 工具箱",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CLannadZSY/Tools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    py_modules=["Py-library"],
)
