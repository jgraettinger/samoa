
import random
import unittest

from samoa.core.murmur_hash import MurmurHash

FIXTURE = '\xceT1\x07\xd7\x1b?\xb7b\x93\xc9}\xec\xc3\xa3^\x11\x8b\xae<\x1e\xd0+\x01\xac\x1eH\xb2\xc6\xa9\xfa\xd4\rZc\xa2\xecz\x9d\xf1<5\xebN\xc05\x04gt\x0e\xd0\xd0\x8fe\xe40f(\xfd\x0b\x84\xc5<\xf0\x1d\xea\xa2a\xe9\xff\xc0\x91\x06\xea\xfbr\xd87\xbdI\xb6t\xb9/\xb2\xf5\xb4 \xea\x0co\x1c\xd0\xd9S\x10\'\xa3t?\xb1\x0c\xd0\xd0\xd5v\xe2%\xddl\xab\x0b\x01\xc9\r\xcd\xbc\xed\x13\xfcMHqCH\x84\xc3\xd8\xc8\xab\x04\x1d\xa4\xa6\x1a\xe6\xbak}\x80S\xfb\x9f8n\xecy{\x19 G\x1e\x1c\xb2"\xa0\xb7P%+\x84\xcc*\xd3P\xc6\x12#Z\xef\xaf\xa9y{q\xfa?\x8c\xdbzl\xe6\xdb\x1a\xaa1\x12B\x98\x86hH\xf3\x1b\x9a\xb3\xe7\xd32\xcbv\x03\xb1\x1c q\x1b\xc3\x8b!\x86pR\x87\xabK$\x838\x03r\xa3\xab\xbafq"YU\x9e\xfd\xef\\\x00MY\x11`\x12\xc0\xbd>\xc0|r&\xa1A\xbeg=}Q\xb0}\xa7)EKb.\x9e\x8e)\xf8\xcc\xe8\x91\x08\xf0\xd78\x9a-\xa4\xdb\xef\xdd\xb3\xb7\xc4\x92%~\xa2\x9c\x9d\xa4c\xe2N\x1a\x90|e\x96J1K\xd8\xceq([\xf9\xae\xf1\xa6\xa3\xe6\xfe\x8f\xd4\xcc\x0e\x87\x8c\x85W(\x19\xb3r\x14 \x9fj\x1fj\xb1\xe2\x18?\xba<x\x8c\xd8\x8d\xbd\xc7\xfd\xa9\x9b\xcc\xedjM\xcd\xd5\xc1A\x01"L\xd8\xde\x16E\x1f~\xec1P\xb4\xdd\xd5\xa7m\xb1\xbf\x1e/5\xe4Ko\xdf\x07\x9a\x83\xe6O\xce\x92I62\xe1\x96\xac\xe7P\x03m\x90b\xcf\xf1\x92\x96R\xce\x82"L\xf5\xa1<\xc6\x13\x1a_5@\x98\xbe9\xbeO\xdc\xa0\xb3)\xd9\xd7\t\xa9H+YK\'\xb4<\xdf\x1fGH>\xd0\xcc)\\\x08?\x9aJ\x02\xeb\xa9\xdc-\xcb\xb9\x18\x80x\xc3\x02d\xa6%\x90/\xf1\x1d\x0c\xe4\xe4\x1d\x0b\xb0T\x0f\x83^\x9d\xf5\xbae\xaf\x1aPO\xf01\xd0Fk({(\xa5J\xe9\x99\xc5\x82\xa8\xb5\x91\x9bC\'C\xae\x8a\xc7\xd8\xc0\x90-\xa1#A\x87S\x07n\xde\xb8\x84\xd6\xfe\x1e H\r\xbfTx\x8c\x97\x8d\xec\xa7\xeeB\xf3\x06$dc\xdb\xc5\xdc\x13\x9fe*\xdb\xd2\x80Z>\xac\x8a\xb5\xf9}\xb0t\x14\xe3\xa8\x8d\xf3\xe4\xb9{E!\xc7\x15\xd3M\xf4\xccx\xfb\xb0\xf9\xde\x91K\xdc\xb2"\xc8\x15\xa2ci#\x15\x93:p\xe9n\xa7\xbd[\xb6\x8c\r\x860\xb5\x1f\xfdt\x12%!\x1dA\x85\x9ep\x0e,d\xb2\xfc\xf4\x88+\x05\xcb\x05(\xed9\xf3\x81\xb7\x92*5\xe4\x87\xea\xc4\xff\xf8-B@\xc6\xd8.^\xebQ\x0c\xb7\xa6\xeb/\xfb;.\xdc\xd7t\x7f<f\x949{\xe8\x16"\xfb\xd6\x88\xd6\x05\x03\x1b\xbc\xcb\xa0\xbf}x\x0e\x05W\xf1\xb6xs\xb1\xc2y]\x07(\x10mH)\xc2\xb9\xda\x13\x19L\xad6\xc5>\x99\xf8c\x98H\x8cUS\xb9;\xa3R\xe9\x128\x03\xb2_\xc0\x96\x1d\x99\xa7\x15\x9b\xbaX\xb7\xc1\x15PO\x86\xce\x9c\x8e_\xe96\xe5"\x88Y\xd5V\xbc"\x9eQ\x8c\xf4\xb0\xd4\x81"7\xb4~+\'\xa0\x8br&\x12\x9d\xd2\x1d\x13\xed\x16\xdcvs\xe0\r\xa3\xcf\x80\xac\x8d\x86uI"<\xc4\x12\x18&\x18\xf2~;_\x92F\xeb\xd3^\xaf\xca\'\xdc\x81\xdd\x0c\xbe\x93\x12\x0e\x015\xc8\xae\xe6\x07S\'o?\xae$\x053\xb4\x8e`L\x1a\xfaA[\xaen\xc1V^E\xd7\'HV\x1cS\xff\xb8/\xe4\x8b\xc5j\xfa\xb8\xfeLF\xfa\x8en?#-\xcaP\x00\xc2\r\xd4,\x13\x9f\x91\xe3\x1c\x94\xa7\xb3\x05c\x17\xf3\x18NK\xdc\xc2\xed\x84M5sp\x94\xde\xf41y\x95c\xe9\x89\xe5n\xb2\x1d\x88\x8a\xaa\x00\x88_F\xa9e\xe8D"N\xc6\x9b\x1cq\xa9\xd0\xdf\xa0\xdc\xbd\xe4\xd6k\x0c\x17\x95\x1eae:8E\x8d\x8b\xe7&\xfa\xcf1#^(\xf9\x1c\xf1\x8d\xaf\xed\xba\xcc5\xba\xaa\xfe\xe1\xdbL^s\xe6]+C\xe6z\xd1\x19\xe2ie\xccs\x84\xdc\xc0p2B\xa5\x05\xe4 %\xe1v\x87U8E\x16\xdf2\xb7R5H\xa6\t\xe6\xde\xaay\x04\xc8p7J,\xf0\x95\n\xc6\x98\x86 \xb3Fd\xd2\xb3\xbbJ5\xd4\xdc\xfc\xdcE+\xb0\xb4B\xd0\x0259\xd8~\x86\xac\xe2\x01\xb5_V\xb7\x02B\xfa\xac\x12/:\xf9~\x7f,\xa9Y\x8c\xcc\x01\x19C\x88i{\xc540\xa6IX\x1c\x82"\xee\xf1$\xca\xf5\xab\x8d\x86\x18rq\xbf^\x9a\xb9]\x1d\xa5V_H\x95\x17f\x12\x863\xb8\x10\x8f\x14\xa4\xcd)\x95\xea\x99\xd1\x85\r\x99x\x823\xe7*\x06\x81\xa9.j\xbch\x12%~\xea\x1d\xe3h\xec\x8b\x8f\xf9\x93\xc8\xe9\xb3\xd7r\xf2rJ\x14\x11\x0e%\x05`\xc3\x96\\\xf2\xb7\x94\xcdJ\xe7>\xd4\xf6I+>\xe9y\xde\xa5\xa4\x01\r\xdb.\x03z\xa9/\xa2\xca\x16{in\x1f\xbd\xdb\xd4\xf7\x1b\xbah\xd7\xc5T\x83\xc8\x17k\xa4\xb8\x0fQ\xd1]'

# Known outputs of MurmurHash3_x64_128(FIXTURE) under different seedings;
#  collected via building/running http://code.google.com/p/smhasher/
EXPECTED_0 = (4496334983152091838, 11108987181450900358L)
EXPECTED_32 = (9281637242514759142L, 1652188564666415302)

class TestMurmurHash(unittest.TestCase):

    def test_single_process_call(self):

        global FIXTURE, EXPECTED_0

        mc = MurmurHash(0)
        mc.process_bytes(FIXTURE)
        self.assertEquals(mc.checksum(), EXPECTED_0)

    def test_multi_process_call(self):

        global FIXTURE, EXPECTED_32

        mc = MurmurHash(32)

        buf = FIXTURE
        while buf:
            t = random.randint(0, 20)
            mc.process_bytes(buf[:t])
            buf = buf[t:]

        self.assertEquals(mc.checksum(), EXPECTED_32)
