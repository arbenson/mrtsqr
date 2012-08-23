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
  cmd = 'python run_tsqr_ir.py '
  cmd += '--input=Stability_test_%d.mseq ' % i
  cmd += '--output=Stability_caqr_%d ' % i
  cmd += '--hadoop=icme-hadoop1 '
  cm.exec_cmd(cmd)

  cmd = 'python run_full_tsqr.py '
  cmd += '--input=Stability_test_%d.mseq ' % i
  cmd += '--output=Stability_full_%d ' % i
  cmd += '--hadoop=icme-hadoop1 '
  cmd += '--ncols=10 '
  cmd += '--schedule=20,20,20'
  cm.exec_cmd(cmd)
