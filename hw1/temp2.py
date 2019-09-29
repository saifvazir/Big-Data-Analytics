l = [(1,2),(2,3)]
#check indegree of all nodes, if no one has 0 indegree then false
pre_reqs = [x[0] for x in l]
non_p = [x[1] for x in l]
p_r_set = set(pre_reqs)
n_p_set = set(non_p)
def firstTest(p_r_set,n_p_set):
	i=0
	for each in p_r_set:
		if each in n_p_set:
			i+=1
		else:
			return each
	if i==len(p_r_set):
		return False
	return True

def 
def getCourse()