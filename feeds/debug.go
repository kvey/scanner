package feeds

import (
	//	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pogodevorg/POGOProtos-go"
)

type DebugFeed struct{}

func (f *DebugFeed) Push(entry interface{}) {

	switch e := entry.(type) {
	default:
		// NOOP: Will not report type
	case *protos.GetMapObjectsResponse:
		cells := e.GetMapCells()

		for _, cell := range cells {
			fmt.Println(cell)

			nearbyPokemons := cell.GetNearbyPokemons()
			if len(nearbyPokemons) > 0 {
				fmt.Println(nearbyPokemons)
			}

			pokemons := cell.GetWildPokemons()
			if len(pokemons) > 0 {
				fmt.Println(pokemons)
			}

			forts := cell.GetForts()
			if len(forts) > 0 {
				fmt.Println(forts)
			}
		}
	}
}
