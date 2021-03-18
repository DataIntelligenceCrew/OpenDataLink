import subprocess
import pandas as pd

sizes = [50,100,250,500,750]
gen_times = list(range(len(sizes)))
gen_sizes = list(range(len(sizes)))

df = pd.DataFrame({"size": [], "time": [], "gen_size": []})

iters = 30

for i in range(len(sizes)):
	for k in range(iters):
		p = subprocess.Popen("./benchmarking --orgsize={}".format(str(sizes[i])),stdout=subprocess.PIPE, shell=True)
		(output, err) = p.communicate()
		p_stats = p.wait()
		w_output = output.split(b'\n')
		time = ""
		size = ""
		for j in range (len(w_output)):
			line = w_output[j].split(b':')
			if line[0] == b'Time':
				time = line[1].decode("utf-8")
			elif line[0] == b'Size':
				size = line[1].decode("utf-8")
		df.loc[len(df.index)] = [sizes[i], float(time), float(size)]
		print(output)
		print("Organization for size {} took {}s to generate ({}/{})".format(sizes[i], time, k, iters))

df.to_csv("organization_benchmark.csv")
