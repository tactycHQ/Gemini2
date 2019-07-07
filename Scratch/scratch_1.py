import numpy as np


x = [1,2,3,4,5]
y = [0,0,0,0,0]
z = [10,20,30,40,50]

a= np.array([[x,z,x],[y,z,z]])

print(a.shape)
print(a[:,:2,:].shape)

