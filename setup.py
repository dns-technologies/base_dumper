from setuptools import setup


setup(
    name="pgpack_dumper",
    version="0.0.0.4",
    package_dir={"": "src"},
    packages=[
        "base_dumper",
        "base_dumper.common",
    ],
    include_package_data=True,
    zip_safe=False,
)
