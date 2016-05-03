class Product
  def initialize(name, brandName, departmentName, categoryName, subcategoryName, model)
    @name     = name
    @brandName   = brandName
    @departmentName = departmentName
    @categoryName = categoryName
    @subcategoryName = subcategoryName
    @model = model
  end

  def name(nome)
  	@name = nome
  end
end