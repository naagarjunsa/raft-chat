import  rsa


def generate_encryption_keys_for_group():
    return rsa.newkeys(256)

def get_string_from_private_key(private_key):
    print("-".join(
        [str(i) for i in [private_key.n, private_key.e, private_key.d, private_key.p, private_key.q]]).encode('utf8'))
    return "-".join(
        [str(i) for i in [private_key.n, private_key.e, private_key.d, private_key.p, private_key.q]]).encode('utf8')


def get_private_key_from_string(private_key_string):
    splitted_list = [int(i) for i in private_key_string.decode('utf8').split('-')]
    return rsa.key.PrivateKey(*splitted_list)

# keys1 = generate_encryption_keys_for_group()
# keys2 = generate_encryption_keys_for_group()
# enc_key = rsa.encrypt(get_string_from_private_key(keys1[1]), keys2[0])

(bob_pub, bob_priv) = rsa.newkeys(512)
message = 'hello Bojfdsnfkjsdbgkjsdbgvkjsdbgkjsdnbkjsndkjbnsdkjgnkjsdngkjsdngkjsbgkjsdbkjsdnbjksdngjsndgjndskjjgnsdkjjgjksjdbgkjjsdgknsdkgnsdkngksdngkjnjdnsnnff' \
          'fjnfjsnjksngksgjksgksgsgnjkjb!'.encode('utf8')
crypto = rsa.encrypt(message, bob_pub)