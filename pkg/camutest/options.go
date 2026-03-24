package camutest

// Options configures a test environment.
type Options struct {
	Instances   int
	UseMinIO    bool
	InstanceIDs []string
}

// Option is a functional option for configuring Options.
type Option func(*Options)

// WithInstances sets the number of server instances to start.
func WithInstances(n int) Option { return func(o *Options) { o.Instances = n } }

// WithMinIO enables MinIO container support (stub: currently uses in-memory S3).
func WithMinIO() Option { return func(o *Options) { o.UseMinIO = true } }

// WithInstanceIDs sets explicit instance IDs for started servers.
func WithInstanceIDs(ids ...string) Option {
	return func(o *Options) {
		o.InstanceIDs = append([]string(nil), ids...)
	}
}
