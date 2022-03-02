import rsa
import pickle

keys = dict()
for i in range(1, 6):
    keys[str(i)] = rsa.newkeys(16)

with open('keystore.obj', 'wb') as keystore:
    pickle.dump(keys, keystore)
