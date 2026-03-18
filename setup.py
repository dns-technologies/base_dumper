from setuptools import setup


setup(
    name="pgpack_dumper",
    version="0.1.0.dev4",
    package_dir={"": "src"},
    packages=[
        "base_dumper",
        "base_dumper.common",
    ],
    include_package_data=True,
    zip_safe=False,
)
