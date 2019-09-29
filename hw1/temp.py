#pre order = 3,9,20,15,7
#inorder = 9,315,20,7
p = [3,9,20,15,7]
inorder = [9,315,20,7]
class Node:
	def __int__(self,val):
		self.val = val
		self.right = None
		self.left = None

def find_tree(preorder,inorder):
	root = Node(preorder[0])
	root.left = getLeftSubTree(preorder,inorder)
	root.right = getRightSubTree(preorder,inorder)

def getLeftSubTree():
	

def getRightSubTree():
	pass