from setuptools import setup, find_namespace_packages

setup(
    name="book_pipeline_consumer_func",
    version="0.1.0",
    packages=find_namespace_packages(include=["book_pipeline_consumer_func", "book_pipeline_consumer_func.*"]),
    include_package_data=True,
    install_requires=[
        "requests>=2.25.1",
        "flask",
        "functions-framework",
        "google-cloud-logging",
        "google-cloud-pubsub",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pandas",
    ],
    python_requires=">=3.7",
    zip_safe=False,
    author="Mohamad",
    author_email="your.email@example.com",
    description="A custom package for the book data pipeline project.",
    url=""
)
