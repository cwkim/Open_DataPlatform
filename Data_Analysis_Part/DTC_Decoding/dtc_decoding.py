dtc = ["0x03,0x00,0x05,0x7E",
        "0xBB,0x04,0x03,0x01",
        "0x4C,0x0E,0x05,0x01",
        "0x1B,0x00,0x02,0x02",
        "0x43,0x15,0x0C,0x01",
       "0x00,0x00,0x00,0x00"]


def dtcdecoder(dtc):
    binaries = ''
    for i in range(2,18,5):
        temp = bin(int(dtc[i:i+2],16))[2:]
        for j in range(8-len(temp)):temp='0'+temp
        binaries+=temp
    temp = (binaries[16:19] + binaries[8:16] + binaries[0:8])
    spn = int(temp, 2)
    fmi = int(binaries[19:24], 2)
    cm = int(binaries[24:25], 2)
    oc = int(binaries[25:], 2)
    print('spn :', spn, ' / fmi :', fmi, ' / cm :', cm, ' / oc :', oc)
    return [spn, fmi, cm, oc]


for i in range(len(dtc)):
    dtcdecoder(dtc[i])