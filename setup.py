from setuptools import setup


setup(
    name="base_dumper",
    version="0.3.0.dev0",
    package_dir={"": "src"},
    packages=[
        "base_dumper",
        "base_dumper.common",
    ],
    include_package_data=True,
    zip_safe=False,
)
