import numpy as np

# Create a bytearray with 1,250 bytes
bytearray1 = bytearray([0b10101010] * 1250)
bytearray2 = bytearray([0b01010101] * 1250)

# Convert bytearrays to numpy arrays
array1 = np.frombuffer(bytearray1, dtype=np.uint8)
array2 = np.frombuffer(bytearray2, dtype=np.uint8)

# Perform the XOR operation using numpy
result = np.bitwise_xor(array1, array2)

# Convert the result back to a bytearray
result_bytearray = bytearray(result)

# Print the first few results for verification
print("Result (first 10 bytes):", [bin(b) for b in result_bytearray[:10]])
