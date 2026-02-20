import nox

PYTHON_VERSIONS = ["3.11", "3.12", "3.13", "3.14"]


@nox.session(python=PYTHON_VERSIONS)
def tests(session):
    session.install(".", "pytest", "pytest-cov")
    session.run("pytest", "--cov", "--cov-report=xml", *session.posargs)
