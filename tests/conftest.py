import pytest
import docker
import ibis
import time

# Constants
SQLFLITE_PORT = 31337


# Function to wait for a specific log message indicating the container is ready
def wait_for_container_log(container, timeout=30, poll_interval=1, ready_message="SQLFlite server - started"):
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the logs from the container
        logs = container.logs().decode('utf-8')

        # Check if the ready message is in the logs
        if ready_message in logs:
            return True

        # Wait for the next poll
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


@pytest.fixture(scope="session")
def sqlflite_server():
    client = docker.from_env()
    container = client.containers.run(
        image="voltrondata/sqlflite:latest",
        name="ibis-sqlflite-test",
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{SQLFLITE_PORT}/tcp": SQLFLITE_PORT},
        environment={"SQLFLITE_USERNAME": "sqlflite_username",
                     "SQLFLITE_PASSWORD": "sqlflite_password",
                     "TLS_ENABLED": "1",
                     "PRINT_QUERIES": "1"
                     },
        stdout=True,
        stderr=True
    )

    # Wait for the container to be ready
    wait_for_container_log(container)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def con(sqlflite_server):
    con = ibis.sqlflite.connect(host="localhost",
                                user="sqlflite_username",
                                password="sqlflite_password",
                                port=SQLFLITE_PORT,
                                use_encryption=True,
                                disable_certificate_verification=True
                                )
    return con
