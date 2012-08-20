import util
cm = util.CommandManager()

for i, scale in enumerate([1,
                           10,
                           100,
                           1000,
                           10000,
                           100000,
                           1000000,
                           10000000,
                           100000000,
                           1000000000,
                           10000000000,
                           100000000000,
                           1000000000000,
                           10000000000000,
                           100000000000000,
                           1000000000000000,
                           10000000000000000]):
  cmd = 'dumbo start parse_and_change.py '
  cmd += '-mat Simple_1M.tmat '
  cmd += '-scale %d ' % scale
  cmd += '-output Stability_test_%d.mseq ' % i
  cmd += '-nummaptasks 40 '
  cmd += '-hadoop icme-hadoop1 '
  cm.exec_cmd(cmd)
