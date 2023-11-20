package autopipe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCmdItems_Wait_zeroLen(t *testing.T) {
	items := cmdItems{}
	require.NoError(t, items.Wait())
}
