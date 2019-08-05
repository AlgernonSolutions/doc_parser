import json
from os import path
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture
def task_event():
    return _read_test_event('event')


@pytest.fixture
def test_documentation():
    return _read_test_event('rds_event')


@pytest.fixture(autouse=True)
def silence_x_ray():
    x_ray_patch_all = 'algernon.aws.lambda_logging.patch_all'
    patch(x_ray_patch_all).start()
    yield
    patch.stopall()


@pytest.fixture
def mock_context():
    context = MagicMock(name='context')
    context.__reduce__ = cheap_mock
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    context.get_remaining_time_in_millis.side_effect = [1000001, 500001, 250000, 0]
    return context


@pytest.fixture(params=['ahl_client_contact', 'community_support'])
def documentation(request):
    file_name = request.param
    return _read_test_html(file_name)


def cheap_mock(*args):
    from unittest.mock import Mock
    return Mock, ()


def _read_test_event(event_name):
    user_home = path.expanduser('~')
    with open(path.join(str(user_home), '.algernon', 'doc_parser', f'{event_name}.json')) as json_file:
        event = json.load(json_file)
        return event


def _read_test_html(event_name):
    user_home = path.expanduser('~')
    with open(path.join(str(user_home), '.algernon', 'doc_parser', f'{event_name}.html')) as html_file:
        return html_file.read()
