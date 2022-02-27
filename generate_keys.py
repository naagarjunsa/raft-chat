import rsa
import pickle

keys = list()
for i in range(5):
    keys.append(rsa.newkeys(16))

with open('keystore.obj', 'wb') as keystore:
    pickle.dump(keys, keystore)
