from setuptools import find_packages, setup

setup(
      name="tap-branch",
      version="0.0.1",
      description="Singer.io tap for extracting data from branch API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_branch"],
      install_requires=[
        "singer-python==6.3.0",
        "requests==2.32.5",
        "backoff==2.2.1",
        "pendulum==3.1.0",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-branch=tap_branch:main
      """,
      packages=find_packages(),
      package_data={
          "tap_branch": ["schemas/*.json"],
      },
      include_package_data=True,
)
