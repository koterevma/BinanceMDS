def get_headers() -> dict[str, str]:
    depth20_headers = _generate_depth20_headers()
    headers = dict(
            trade= 'ts, trade id, price, amount, maker side\n',
            depth20='ts, ' + ', '.join(depth20_headers) + '\n'
        )
    return headers

def _generate_depth20_headers() -> list[str]:
    headers = []
    for i in range(20):
        headers.append(f'bid[{i}].price')
        headers.append(f'bid[{i}].amount')

    for i in range(20):
        headers.append(f'ask[{i}].price')
        headers.append(f'ask[{i}].amount')

    return headers
