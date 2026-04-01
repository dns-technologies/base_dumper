from setuptools import setup


setup(
    name="pgpack_dumper",
    version="0.2.0.dev1",
    package_dir={"": "src"},
    packages=[
        "base_dumper",
        "base_dumper.common",
    ],
    include_package_data=True,
    zip_safe=False,
)
