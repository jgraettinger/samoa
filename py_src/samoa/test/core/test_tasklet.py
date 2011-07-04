
import unittest

from samoa.core.proactor import Proactor
from samoa.core.tasklet import Tasklet
from samoa.core.tasklet_group import TaskletGroup

class ATasklet(Tasklet):

    def __init__(self, out, loop):
        Tasklet.__init__(self, Proactor.get_proactor().serial_io_service())
        self.set_tasklet_name('test-tasklet')

        self.out = out
        self.out.append('not started')
        self.loop = loop

    def __del__(self):
        self.out.append('destroyed')

    def run_tasklet(self):
        proactor = Proactor.get_proactor()

        self.out[0] = 'running'

        while self.loop:
            yield proactor.sleep(1)

        self.out[0] = 'halted'
        yield

    def halt_tasklet(self):
        self.loop = False


class TestTasklet(unittest.TestCase):

    def test_managed_looped_tasklet(self):

        proactor = Proactor.get_proactor()

        def test():

            out = []
            tlet = ATasklet(out, True)
            tlet_group = TaskletGroup()
            tlet_group.start_managed_tasklet(tlet)

            yield proactor.sleep(5)
            self.assertEquals(out, ['running'])

            tlet_group.cancel_group()

            yield proactor.sleep(5)
            self.assertEquals(out, ['halted'])

            # ours is the only remaining reference to the tasklet
            del tlet
            self.assertEquals(out, ['halted', 'destroyed'])
            yield

        proactor.spawn(test)
        proactor.run()

    def test_orphaned_looped_tasklet(self):

        proactor = Proactor.get_proactor()

        def test():

            out = []
            tlet_group = TaskletGroup()
            tlet_group.start_orphaned_tasklet(ATasklet(out, True))

            yield proactor.sleep(5)
            self.assertEquals(out, ['running'])

            tlet_group.cancel_group()

            # no references should remain
            yield proactor.sleep(5)
            self.assertEquals(out, ['halted', 'destroyed'])
            yield

        proactor.spawn(test)
        proactor.run()

    def test_managed_short_tasklet(self):

        proactor = Proactor.get_proactor()

        def test():

            out = []
            tlet = ATasklet(out, False)
            TaskletGroup().start_managed_tasklet(tlet)

            yield proactor.sleep(5)

            # the tasklet has run, and already finished
            self.assertEquals(out, ['halted'])

            # ours is the only remaining reference to the tasklet
            del tlet
            self.assertEquals(out, ['halted', 'destroyed'])
            yield

    def test_orphaned_short_tasklet(self):
        
        proactor = Proactor.get_proactor()

        def test():

            out = []
            TaskletGroup().start_orphaned_tasklet(ATasklet(out, False))

            yield proactor.sleep(5)

            # the tasklet has run, finished, and been finalized
            self.assertEquals(out, ['halted', 'destroyed'])
            yield

        proactor.spawn(test)
        proactor.run()

    def test_managed_without_reference(self):

        proactor = Proactor.get_proactor()

        def test():

            out = []
            tlet_group = TaskletGroup()
            tlet_group.start_managed_tasklet(ATasklet(out, True))

            # tasklet is destroyed before it's started
            yield proactor.sleep(5)
            self.assertEquals(out, ['not started', 'destroyed'])

            yield

        proactor.spawn(test)
        proactor.run()

