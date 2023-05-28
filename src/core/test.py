class FileNode():
    def __init__(self,filename: str,file_id: str,isFile: bool):
        self.children = []
        self.isFile = isFile
        self.filename = filename
        self.file_id = file_id
    
    def __str__(self) -> str:
        return(
            "FileNode { "+
            "filename: "+self.filename+
            ", children: "+str(self.children)+
            ", file_id: "+self.file_id+
            ", isFile: "+str(self.isFile)+
            " }"
        )
tree = {
    "root": FileNode("root","id1",False),
    "root/test":FileNode("test","id2",False),
    "root/test/again":FileNode("again","id10",False),
    "root/test/poopy.mp3": FileNode("poopy.mp3","id3",True),
    "root/test/poop.mp3": FileNode("poop.mp3","id4",True),
    "root/test/poo.mp3": FileNode("poo.mp3","id5",True),
    "root/test/po.mp3": FileNode("po.mp3","id6",True),
    "root/test/p.mp3": FileNode("p.mp3","id7",True),
    "root/test.mp4": FileNode("test.mp3","id8",True),
    "root/tes.mp4": FileNode("tes.mp3","id9",True),
}

tree["root/test"].children.append("root/test/poopy.mp3")
tree["root/test"].children.append("root/test/again")
tree["root/test"].children.append("root/test/poop.mp3")
tree["root/test"].children.append("root/test/poo.mp3")
tree["root/test"].children.append("root/test/po.mp3")
tree["root/test"].children.append("root/test/p.mp3")
tree["root"].children.append("root/test.mp4")
tree["root"].children.append("root/tes.mp4")

def recurDelete(dir: str):
    for child in tree[dir].children:
        if child in tree:
            if tree[child].isFile:
                del tree[child]
            elif (not tree[child].isFile and tree[child].children == []):
                del tree[child]
            else:
                recurDelete(tree[child], type(string))
    del tree[dir]
def driver():
    recurDelete("root/test")
    print(tree)


if __name__ == "__main__":
    driver()