import random
import os

if __name__ == "__main__":
    dir = os.path.dirname(os.path.realpath(__file__))
    file = f"{dir}/copy.csv"
    with open(file, 'w') as f:
        f.write("id,value\n")
        for x in range(4096):
            x = random.randint(1, 128_000_000_000) # 128B, why not
            f.write(f"{x},\"email-{x}@test.com\"\n")
