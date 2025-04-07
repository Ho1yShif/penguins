from ascend.resources import ref, transform

@transform(inputs=[ref("read_penguins")])
def add_culmen_ratio(read_penguins):
  # add your Transform logic here
  return read_penguins
