package template

// Context holds data passed to templates during rendering.
type Context struct {
	// CurrentModel is the name of the model currently being processed
	CurrentModel string

	// Schema is the default schema for table references
	Schema string

	// IsIncremental indicates whether the model is being executed in incremental mode
	IsIncremental bool

	// CurrentModelTable is the fully qualified table name for the current model being processed
	// e.g., "schema.table_name" or just "table_name"
	CurrentModelTable string

	// Vars contains project variables available in templates
	Vars map[string]interface{}

	// Config contains configuration values accessible via config() function
	Config map[string]interface{}

	// Sources maps source names to their table definitions
	// Structure: Sources[sourceName][tableName] = qualifiedName
	Sources map[string]map[string]string
}

// ContextOption configures a Context.
type ContextOption func(*Context)

// NewContext creates a new template context with the given options.
func NewContext(opts ...ContextOption) *Context {
	ctx := &Context{
		Vars:    make(map[string]interface{}),
		Config:  make(map[string]interface{}),
		Sources: make(map[string]map[string]string),
	}

	for _, opt := range opts {
		opt(ctx)
	}

	return ctx
}

// WithVars sets the variables for the context.
func WithVars(vars map[string]interface{}) ContextOption {
	return func(c *Context) {
		c.Vars = vars
	}
}

// WithConfig sets the configuration for the context.
func WithConfig(config map[string]interface{}) ContextOption {
	return func(c *Context) {
		c.Config = config
	}
}

// WithSchema sets the default schema for the context.
func WithSchema(schema string) ContextOption {
	return func(c *Context) {
		c.Schema = schema
	}
}

// WithCurrentModel sets the current model being processed.
func WithCurrentModel(model string) ContextOption {
	return func(c *Context) {
		c.CurrentModel = model
	}
}

// WithSources sets the source definitions for the context.
func WithSources(sources map[string]map[string]string) ContextOption {
	return func(c *Context) {
		c.Sources = sources
	}
}

// WithIsIncremental sets the incremental execution flag for the context.
func WithIsIncremental(isIncremental bool) ContextOption {
	return func(c *Context) {
		c.IsIncremental = isIncremental
	}
}

// WithCurrentModelTable sets the fully qualified table name for the current model.
func WithCurrentModelTable(tableName string) ContextOption {
	return func(c *Context) {
		c.CurrentModelTable = tableName
	}
}
