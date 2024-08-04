import json


def redis_xread_to_python(data) -> list:
    '''
    Customized function to convert redis xread data to python dictionary
    '''
    try:
        _, _dl = data[0]
        _result = []
        for _d in _dl:
            item_id, td = _d
            _temp = {k.decode(): v.decode() for k, v in td.items()}
            _temp["entry_id"] = item_id.decode()
            _result.append(_temp)
    except ValueError:
        print(ValueError)
    except KeyError:
        print(KeyError)
    return _result


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
