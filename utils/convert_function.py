import json


def redis_subscribe_to_python(data):
    '''
    Customized function to convert redis subscribe data to python dictionary
    '''
    data = json.loads(data["data"].decode())
    return data


def redis_xrange_to_python(data):
    '''
    Customized function to convert redis xrange data to python dictionary
    '''
    try:
        _, _dl = data[0]
        _temp = {k.decode(): v.decode() for k, v in _dl.items()}
    except ValueError:
        print(ValueError)
    except KeyError:
        print(KeyError)
    return [_temp]
